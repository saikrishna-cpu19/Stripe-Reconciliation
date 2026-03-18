# recon_dashboard_github_persist.py
# --- Reconciliation Dashboard with GitHub-based persistence ---
# - All previous features preserved from finalised18_corrected.py
# - SQLite REMOVED (ephemeral on Streamlit Cloud — data was lost on restart)
# - NEW: GitHub persistence — saves 4 Excel files directly to your GitHub repo:
#     data/Unmatched_History.xlsx
#     data/RRN_Match_History.xlsx
#     data/Daily_Reconciliation_Log.xlsx
#     data/Audit_Trail.xlsx
# - On every app startup, these files are auto-loaded from GitHub → session restored
# - Users never lose previous day records again
# - HOW TO SET UP:
#     1. In your GitHub repo, create a folder called: data/
#        (add a blank .gitkeep file inside it so the folder exists)
#     2. Generate a GitHub Personal Access Token:
#        GitHub → Settings → Developer Settings → Personal Access Tokens → Fine-grained
#        Give it: Contents = Read & Write, on your repo only
#     3. In Streamlit Cloud → App Settings → Secrets, add:
#        GITHUB_TOKEN = "ghp_xxxxxxxxxxxx"
#        GITHUB_REPO  = "your-username/your-repo-name"
#        GITHUB_BRANCH = "main"   (or master — whatever your default branch is)

import io, re, json, base64
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd
import requests
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ===================== Page Config =====================
st.set_page_config(page_title="Reconciliation Dashboard", layout="wide")
try:
    st.sidebar.image("image.png", use_container_width=True)
except Exception:
    pass
theme = st.sidebar.radio("Choose Theme", ["Light", "Dark"])
if theme == "Dark":
    st.markdown(
        """
        <style>
            .stApp { background-color: #0e1117; color: #fafafa; }
            .css-1d391kg, .stMarkdown, .stText, .stDataFrame { color: #fafafa !important; }
        </style>
        """,
        unsafe_allow_html=True,
    )

# ===================== Sidebar Inputs ==================
st.sidebar.header("📁 Upload Files")
n1_file     = st.sidebar.file_uploader("Upload N1 File",                          type=["xlsx","xls","csv"])
stripe_file = st.sidebar.file_uploader("Upload Stripe File",                      type=["xlsx","xls","csv"])
pi_file     = st.sidebar.file_uploader("Upload PI_Numbers File",                  type=["xlsx","xls","csv"])
history_file    = st.sidebar.file_uploader("Upload Unmatched_History (optional)", type=["xlsx","xls","csv"])
log_file        = st.sidebar.file_uploader("Upload Daily_Reconciliation_Log (optional)", type=["xlsx","xls","csv"])
rrn_history_file= st.sidebar.file_uploader("Upload RRN_Match_History (optional)", type=["xlsx","xls","csv"])

as_of_date = st.sidebar.date_input("As-of date for this run", value=date.today())

# ===================== GitHub Persistence Helpers =====================
# Paths inside your GitHub repo where data files are saved
GH_PATHS = {
    "unmatched_history":  "data/Unmatched_History.xlsx",
    "rrn_match_history":  "data/RRN_Match_History.xlsx",
    "daily_log":          "data/Daily_Reconciliation_Log.xlsx",
    "audit_trail":        "data/Audit_Trail.xlsx",
}

def _gh_headers():
    token = st.secrets.get("GITHUB_TOKEN", "")
    # Fine-grained tokens (github_pat_) require "Bearer", classic tokens use "token"
    # Using "Bearer" works for BOTH token types
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}

def _gh_repo():
    return st.secrets.get("GITHUB_REPO", "")

def _gh_branch():
    return st.secrets.get("GITHUB_BRANCH", "main")

def github_read_excel(gh_path: str, sheet_name=None) -> pd.DataFrame:
    """Download an Excel file from GitHub and return as DataFrame. Returns empty DF if not found."""
    repo   = _gh_repo()
    branch = _gh_branch()
    url    = f"https://api.github.com/repos/{repo}/contents/{gh_path}?ref={branch}"
    try:
        resp = requests.get(url, headers=_gh_headers(), timeout=15)
        if resp.status_code == 404:
            return pd.DataFrame()   # file doesn't exist yet — first run
        resp.raise_for_status()
        content_b64 = resp.json().get("content", "")
        raw_bytes   = base64.b64decode(content_b64)
        bio         = io.BytesIO(raw_bytes)
        if sheet_name:
            try:
                return pd.read_excel(bio, sheet_name=sheet_name, engine="openpyxl")
            except Exception:
                bio.seek(0)
                return pd.read_excel(bio, engine="openpyxl")
        return pd.read_excel(bio, engine="openpyxl")
    except Exception as e:
        st.warning(f"⚠️ Could not read {gh_path} from GitHub: {e}")
        return pd.DataFrame()

def github_write_excel(gh_path: str, sheets: dict):
    """Write a multi-sheet Excel file to GitHub (create or update)."""
    repo   = _gh_repo()
    branch = _gh_branch()
    # Build Excel bytes
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        any_written = False
        for sheet, df in sheets.items():
            if df is not None and not df.empty:
                sanitize_for_excel(df).to_excel(writer, sheet_name=sheet[:31], index=False)
                any_written = True
        # openpyxl requires at least one visible sheet — write placeholder if all empty
        if not any_written:
            pd.DataFrame({"status": ["No data yet"]}).to_excel(writer, sheet_name="Placeholder", index=False)
    bio.seek(0)
    new_content_b64 = base64.b64encode(bio.read()).decode()

    # Check if file already exists (need its SHA to update)
    url  = f"https://api.github.com/repos/{repo}/contents/{gh_path}"
    get_resp = requests.get(url, headers=_gh_headers(), params={"ref": branch}, timeout=15)
    sha  = get_resp.json().get("sha") if get_resp.status_code == 200 else None

    payload = {
        "message": f"recon-app: update {gh_path} [{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}]",
        "content": new_content_b64,
        "branch":  branch,
    }
    if sha:
        payload["sha"] = sha   # required for updating existing file

    put_resp = requests.put(url, headers=_gh_headers(), json=payload, timeout=30)
    if put_resp.status_code not in (200, 201):
        st.warning(f"⚠️ Could not save {gh_path} to GitHub: {put_resp.json().get('message','unknown error')}")
        return False
    return True

# ===================== File & Data Helpers ======================
def robust_read_csv(file_obj) -> pd.DataFrame:
    try:
        return pd.read_csv(file_obj, sep=None, engine="python", encoding="utf-8")
    except Exception:
        file_obj.seek(0)
        return pd.read_csv(file_obj, sep=None, engine="python", encoding="latin1")

def load_file(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith(".csv"):
        return robust_read_csv(file)
    elif name.endswith(".xlsx"):
        return pd.read_excel(file, engine="openpyxl")
    elif name.endswith(".xls"):
        try:
            return pd.read_excel(file, engine="xlrd")
        except Exception as e:
            st.warning(f"Could not read .xls with xlrd: {e}. Convert to .xlsx/.csv.")
            raise
    else:
        raise ValueError("Unsupported file type")

def find_first_matching_col(df: pd.DataFrame, candidates, contains=False) -> Optional[str]:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    lower_map = {c.lower(): c for c in cols}
    if contains:
        for cand in candidates:
            cl = cand.lower()
            for c in cols:
                if cl in c.lower():
                    return c
        return None
    else:
        for cand in candidates:
            if cand.lower() in lower_map:
                return lower_map[cand.lower()]
        return None

def clean_amount(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.replace("-", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

ILLEGAL_CHAR_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
def _sanitize_obj(v):
    if pd.isna(v):
        return v
    s = str(v)
    if ILLEGAL_CHAR_RE.search(s):
        s = ILLEGAL_CHAR_RE.sub("", s)
    return s

def sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    out.columns = [ILLEGAL_CHAR_RE.sub("", str(c)) for c in out.columns]
    obj_cols = out.select_dtypes(include=["object","string"]).columns
    for c in obj_cols:
        out[c] = out[c].apply(_sanitize_obj)
    return out

def safe_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/*?\[\]]', "_", str(name))
    return name[:31] if len(name) > 31 else name

def to_excel_bytes(sheets: dict, dashboard_sheet_name: str = None, dashboard_df: pd.DataFrame = None) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        any_written = False
        if dashboard_sheet_name is not None and dashboard_df is not None and not dashboard_df.empty:
            sanitize_for_excel(dashboard_df).to_excel(writer, sheet_name=safe_sheet_name(dashboard_sheet_name), index=False)
            any_written = True
        for n, d in sheets.items():
            if d is not None and not (isinstance(d, pd.DataFrame) and d.empty):
                sanitize_for_excel(d).to_excel(writer, sheet_name=safe_sheet_name(n), index=False)
                any_written = True
        if not any_written:
            pd.DataFrame({"status": ["No data yet"]}).to_excel(writer, sheet_name="Placeholder", index=False)
    bio.seek(0)
    return bio.read()

def convert_df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def extract_date_from_filename(file_obj):
    name = getattr(file_obj, 'name', '')
    m = re.search(r'(20\d{2})-(\d{2})-(\d{2})', name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def extract_date_from_n1_filename(n1_streamlit_file) -> Optional[str]:
    return extract_date_from_filename(n1_streamlit_file)

def append_daily_log(recon_log_df, as_of_date_param, n1_file_obj,
                     n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                     stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct):
    file_date    = extract_date_from_filename(n1_file_obj)
    date_for_log = pd.to_datetime(file_date) if file_date else pd.to_datetime(as_of_date_param)
    row = {
        "Date":                    date_for_log,
        "N1 Total":                int(n1_total),
        "N1 Matched Count":        int(n1_matched_count),
        "N1 Matched Amount":       float(n1_matched_amt),
        "N1 Unmatched Count":      int(n1_unmatched_count),
        "N1 Reconciliation %":     float(n1_recon_pct),
        "Stripe Total":            int(stripe_total),
        "Stripe Matched Count":    int(stripe_matched_count),
        "Stripe Matched Amount":   float(stripe_matched_amt),
        "Stripe Unmatched Count":  int(stripe_unmatched_count),
        "Stripe Reconciliation %": float(stripe_recon_pct),
    }
    out = pd.concat([recon_log_df, pd.DataFrame([row])], ignore_index=True)
    out = out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    return out

def append_daily_log_from_n1(recon_log_df, as_of_date_param, n1_file_obj,
                              n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                              stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct):
    return append_daily_log(recon_log_df, as_of_date_param, n1_file_obj,
                            n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                            stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct)

# ===================== Session State ============================
ss = st.session_state
for k, v in {
    "recon_history":       {},
    "hist_n1_unmatched":   pd.DataFrame(),
    "hist_stripe_unmatched": pd.DataFrame(),
    "recent_rrn_clears":   pd.DataFrame(),
    "rrn_match_history":   pd.DataFrame(),
    "recon_log_df":        pd.DataFrame(),
    "audit_trail_df":      pd.DataFrame(),
    "chat_history":        [],
    "global_filters":      {},
}.items():
    if k not in ss:
        ss[k] = v

# ===================== AUTO-RESTORE FROM GITHUB ON STARTUP ======
# This block runs ONCE per session (not on every rerun).
# It loads all previously saved data from your GitHub repo into session_state.
# This is what ensures yesterday's records are available today.
if "github_loaded" not in ss:
    with st.spinner("🔄 Restoring previous session data from GitHub..."):
        try:
            # 1. Unmatched History (N1 + Stripe)
            _uh = github_read_excel(GH_PATHS["unmatched_history"])
            if not _uh.empty:
                # Try sheet-specific reads
                _uh_bio_resp = requests.get(
                    f"https://api.github.com/repos/{_gh_repo()}/contents/{GH_PATHS['unmatched_history']}?ref={_gh_branch()}",
                    headers=_gh_headers(), timeout=15
                )
                if _uh_bio_resp.status_code == 200:
                    _raw = base64.b64decode(_uh_bio_resp.json().get("content",""))
                    _bio = io.BytesIO(_raw)
                    try:
                        ss.hist_n1_unmatched = sanitize_for_excel(
                            pd.read_excel(_bio, sheet_name="N1_Unmatched", engine="openpyxl"))
                    except Exception:
                        pass
                    _bio.seek(0)
                    try:
                        ss.hist_stripe_unmatched = sanitize_for_excel(
                            pd.read_excel(_bio, sheet_name="Stripe_Unmatched", engine="openpyxl"))
                    except Exception:
                        pass

            # 2. RRN Match History
            _rrn = github_read_excel(GH_PATHS["rrn_match_history"], sheet_name="RRN_Match_History")
            if not _rrn.empty:
                ss.rrn_match_history = sanitize_for_excel(_rrn)

            # 3. Daily Reconciliation Log
            _log = github_read_excel(GH_PATHS["daily_log"], sheet_name="Reconciliation_Log")
            if not _log.empty:
                if "Date" in _log.columns:
                    _log["Date"] = pd.to_datetime(_log["Date"])
                ss.recon_log_df = _log

            # 4. Audit Trail
            _audit = github_read_excel(GH_PATHS["audit_trail"], sheet_name="Audit_Trail")
            if not _audit.empty:
                ss.audit_trail_df = _audit

        except Exception as e:
            st.warning(f"⚠️ Could not restore from GitHub: {e}. Starting fresh session.")

    ss["github_loaded"] = True   # mark done — won't run again this session

# ===================== Save-to-GitHub Helper ====================
def save_all_to_github():
    """
    Called after every reconciliation run.
    Pushes all 4 persistent data files back to GitHub.
    This is what makes data survive across days.
    """
    results = {}

    # 1. Unmatched History
    results["unmatched_history"] = github_write_excel(
        GH_PATHS["unmatched_history"],
        {"N1_Unmatched": ss.hist_n1_unmatched, "Stripe_Unmatched": ss.hist_stripe_unmatched}
    )

    # 2. RRN Match History
    results["rrn_match_history"] = github_write_excel(
        GH_PATHS["rrn_match_history"],
        {"RRN_Match_History": ss.rrn_match_history}
    )

    # 3. Daily Reconciliation Log
    results["daily_log"] = github_write_excel(
        GH_PATHS["daily_log"],
        {"Reconciliation_Log": ss.recon_log_df}
    )

    # 4. Audit Trail
    results["audit_trail"] = github_write_excel(
        GH_PATHS["audit_trail"],
        {"Audit_Trail": ss.audit_trail_df}
    )

    return results

# ===================== Main Title ===============================
st.title("🔍 Reconciliation Dashboard")

# Placeholders
n1_matched = pd.DataFrame(); stripe_matched = pd.DataFrame()
n1_unmatched = pd.DataFrame(); stripe_unmatched = pd.DataFrame()
summary_df = pd.DataFrame()
n1_total = stripe_total = 0
n1_matched_amt = stripe_matched_amt = 0.0
n1_recon_pct = stripe_recon_pct = 0.0
n1_df = pd.DataFrame(); stripe_df = pd.DataFrame(); pi_df = pd.DataFrame()

# ===================== Global Filters (UX) ======================
with st.sidebar.expander("🔎 Global Filters", expanded=False):
    gf_date_from   = st.date_input("From date (as_of / created)", value=None)
    gf_date_to     = st.date_input("To date (as_of / created)",   value=None)
    gf_merchant_kw = st.text_input("Merchant contains:")
    gf_category    = st.multiselect("Category (Stripe reporting_category)", ["charge","refund","refund_failure"])
    c1, c2 = st.columns(2)
    with c1:
        gf_min_amt = st.number_input("Min amount", value=0.0, step=1.0, format="%.2f")
    with c2:
        gf_max_amt = st.number_input("Max amount (0 = no max)", value=0.0, step=1.0, format="%.2f")
    if st.button("Clear All Filters", use_container_width=True):
        gf_date_from = None; gf_date_to = None; gf_merchant_kw = ""; gf_category = []; gf_min_amt = 0.0; gf_max_amt = 0.0
    ss.global_filters = {
        "date_from": gf_date_from, "date_to": gf_date_to,
        "merchant_kw": gf_merchant_kw.strip(), "category": gf_category,
        "min_amt": gf_min_amt, "max_amt": gf_max_amt
    }

def apply_global_filters(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    gf  = ss.global_filters or {}
    date_cols = [c for c in ["Date","as_of_date","created","created_at","created_utc","transaction_date"] if c in out.columns]
    if date_cols:
        dtcol = date_cols[0]
        dts   = pd.to_datetime(out[dtcol], errors="coerce")
        if gf.get("date_from"): out = out[dts >= pd.to_datetime(gf["date_from"])]
        if gf.get("date_to"):   out = out[dts <= pd.to_datetime(gf["date_to"]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    merch_col = None
    for c in ["merchant","merchant_name","merchant id","merchant_id","statement_descriptor"]:
        if c in out.columns:
            merch_col = c; break
    if merch_col and gf.get("merchant_kw"):
        out = out[out[merch_col].astype(str).str.contains(gf["merchant_kw"], case=False, na=False)]
    if "reporting_category" in out.columns and gf.get("category"):
        out = out[out["reporting_category"].astype(str).str.lower().isin(gf["category"])]
    amt_col = ("effective_auth_amount" if dataset=="N1" and "effective_auth_amount" in out.columns
               else ("net" if "net" in out.columns else None))
    if amt_col:
        vals = pd.to_numeric(out[amt_col], errors="coerce")
        if gf.get("min_amt", 0) > 0: out = out[vals >= gf["min_amt"]]
        if gf.get("max_amt", 0) > 0: out = out[vals <= gf["max_amt"]]
    return out

# ===================== Data Quality Gate ========================
with st.expander("🧪 Data Quality Gate (after upload)"):
    if n1_file or stripe_file or pi_file:
        st.write("This section will summarize key sanity checks once files load.")
    else:
        st.caption("Upload files to see validation.")

# ===================== RUN RECON ================================
if n1_file and stripe_file and pi_file:
    with st.spinner("Reconciling data..."):
        # Load files
        n1_df     = load_file(n1_file);     stripe_df = load_file(stripe_file); pi_df = load_file(pi_file)
        n1_df     = sanitize_for_excel(n1_df)
        stripe_df = sanitize_for_excel(stripe_df)
        pi_df     = sanitize_for_excel(pi_df)

        # Optional history uploads (manual override)
        if history_file:
            try:
                if history_file.name.lower().endswith(".csv"):
                    ss.hist_n1_unmatched = sanitize_for_excel(robust_read_csv(history_file))
                else:
                    try:
                        ss.hist_n1_unmatched = sanitize_for_excel(pd.read_excel(history_file, sheet_name="N1_Unmatched", engine="openpyxl"))
                    except Exception:
                        ss.hist_n1_unmatched = sanitize_for_excel(pd.read_excel(history_file, engine="openpyxl"))
                    try:
                        ss.hist_stripe_unmatched = sanitize_for_excel(pd.read_excel(history_file, sheet_name="Stripe_Unmatched", engine="openpyxl"))
                    except Exception:
                        pass
            except Exception as e:
                st.warning(f"Could not read Unmatched_History: {e}")

        if log_file:
            try:
                if log_file.name.lower().endswith(".csv"):
                    ss.recon_log_df = robust_read_csv(log_file)
                    if "Date" in ss.recon_log_df.columns:
                        ss.recon_log_df["Date"] = pd.to_datetime(ss.recon_log_df["Date"])
                else:
                    try:
                        ss.recon_log_df = pd.read_excel(log_file, sheet_name="Reconciliation_Log", engine="openpyxl", parse_dates=["Date"])
                    except Exception:
                        ss.recon_log_df = pd.read_excel(log_file, engine="openpyxl")
                        if "Date" in ss.recon_log_df.columns:
                            ss.recon_log_df["Date"] = pd.to_datetime(ss.recon_log_df["Date"])
            except Exception as e:
                st.warning(f"Could not read Daily_Reconciliation_Log: {e}")

        if rrn_history_file:
            try:
                if rrn_history_file.name.lower().endswith(".csv"):
                    ss.rrn_match_history = sanitize_for_excel(robust_read_csv(rrn_history_file))
                else:
                    ss.rrn_match_history = sanitize_for_excel(pd.read_excel(rrn_history_file, engine="openpyxl"))
            except Exception as e:
                st.warning(f"Could not read RRN_Match_History: {e}")

        # Trim columns
        n1_df.columns     = n1_df.columns.str.strip()
        stripe_df.columns = stripe_df.columns.str.strip()
        pi_df.columns     = pi_df.columns.str.strip()

        # Filter Stripe categories
        if "reporting_category" in stripe_df.columns:
            stripe_df = stripe_df[stripe_df["reporting_category"].astype(str).str.lower().isin(["charge","refund","refund_failure"])]

        # Clean amounts
        if "effective_auth_amount" in n1_df.columns:     n1_df["effective_auth_amount"]     = clean_amount(n1_df["effective_auth_amount"])
        if "net" in stripe_df.columns:                   stripe_df["net"]                   = clean_amount(stripe_df["net"])
        if "effective_auth_amount" in pi_df.columns:     pi_df["effective_auth_amount"]      = clean_amount(pi_df["effective_auth_amount"])

        # Match keys
        if {"payment_reference_number","effective_auth_amount"}.issubset(n1_df.columns):
            n1_df["match_key"] = n1_df["payment_reference_number"].astype(str) + "_" + n1_df["effective_auth_amount"].astype(str)
        if {"payment_reference_number","effective_auth_amount"}.issubset(pi_df.columns):
            pi_df["match_key"] = pi_df["payment_reference_number"].astype(str) + "_" + pi_df["effective_auth_amount"].astype(str)
        if "payment_intent_id" in stripe_df.columns and "net" in stripe_df.columns:
            stripe_df["match_key_intent"] = stripe_df["payment_intent_id"].astype(str) + "_" + stripe_df["net"].astype(str)
        if "source_id" in stripe_df.columns and "net" in stripe_df.columns:
            stripe_df["match_key_source"] = stripe_df["source_id"].astype(str) + "_" + stripe_df["net"].astype(str)

        # N1 → Stripe matching (OR logic)
        cond_intent     = n1_df.get("match_key", pd.Series([], dtype=str)).isin(stripe_df.get("match_key_intent", pd.Series([], dtype=str)))
        cond_source     = n1_df.get("match_key", pd.Series([], dtype=str)).isin(stripe_df.get("match_key_source", pd.Series([], dtype=str)))
        n1_matched_mask = cond_intent | cond_source
        n1_matched      = n1_df[n1_matched_mask].copy()
        n1_unmatched    = n1_df[~n1_matched_mask].copy()
        n1_matched["Matched_From"] = "Stripe"; n1_matched["remarks"] = "matched"
        if "status" in n1_unmatched.columns:
            n1_unmatched["remarks"] = n1_unmatched["status"].apply(
                lambda x: "Pending Transactions" if str(x).strip().upper() == "PENDING" else "Need to Investigate")
        else:
            n1_unmatched["remarks"] = "Need to Investigate"
        n1_unmatched["Matched_From"] = "N1"

        # Stripe → (N1 + PI) matching
        combined_keys = pd.concat([
            n1_df.get("match_key", pd.Series([], dtype=str)),
            pi_df.get("match_key", pd.Series([], dtype=str))
        ], ignore_index=True)

        def stripe_match_key(row):
            cat      = str(row.get("reporting_category","")).strip().lower()
            net_str  = str(row.get("net",""))
            intent_id= str(row.get("payment_intent_id",""))
            source_id= str(row.get("source_id",""))
            if cat == "charge":
                return f"{intent_id}_{net_str}"
            elif cat in ["refund","refund_failure"]:
                source_key = f"{source_id}_{net_str}"; intent_key = f"{intent_id}_{net_str}"
                if source_key in combined_keys.values: return source_key
                if intent_key in combined_keys.values: return intent_key
                return None
            return None

        stripe_df["stripe_match_key"] = stripe_df.apply(stripe_match_key, axis=1)
        stripe_matched_mask = stripe_df["stripe_match_key"].isin(n1_df.get("match_key", pd.Series([], dtype=str)))
        stripe_matched   = stripe_df[stripe_matched_mask].copy()
        stripe_unmatched = stripe_df[~stripe_matched_mask].copy().reset_index(drop=True)
        stripe_matched["Matched_From"] = "N1"; stripe_matched["remarks"] = "matched"
        pi_match_mask = stripe_unmatched["stripe_match_key"].isin(pi_df.get("match_key", pd.Series([], dtype=str)))
        stripe_unmatched["Matched_From"] = "Stripe"; stripe_unmatched["remarks"] = ""
        stripe_unmatched.loc[pi_match_mask, "Matched_From"] = "PI_Numbers"
        stripe_unmatched.loc[pi_match_mask, "remarks"]      = "Dispute Write Off/Chargeback Credit"

        def set_stripe_remarks(row):
            cat = str(row.get("reporting_category","")).strip().lower()
            if row.get("remarks"): return row["remarks"]
            if cat == "charge": return "Need to Investigate"
            if cat in ["refund","refund_failure"]: return "Dispute Write Off/Chargeback Credit"
            return ""
        stripe_unmatched["remarks"] = stripe_unmatched.apply(set_stripe_remarks, axis=1)

        # RRN matching vs cumulative N1 unmatched history
        if not ss.hist_n1_unmatched.empty and "as_of_date" not in ss.hist_n1_unmatched.columns:
            ss.hist_n1_unmatched["as_of_date"] = pd.NaT
        hist_rrn_col   = find_first_matching_col(ss.hist_n1_unmatched, ["retrieval_reference_number","RRN","retrieval reference number"])
        stripe_rrn_col = (find_first_matching_col(stripe_df, ["payment_metadata[RRN]","RRN","rrn"], contains=True)
                          or find_first_matching_col(stripe_df, ["payment_metadata[RRN]","RRN","rrn"]))
        stripe_file_date = extract_date_from_filename(stripe_file) or str(as_of_date)

        if not ss.hist_n1_unmatched.empty and hist_rrn_col and stripe_rrn_col:
            rrn_mask          = ss.hist_n1_unmatched[hist_rrn_col].astype(str).isin(stripe_df[stripe_rrn_col].astype(str))
            recent_rrn_clears = ss.hist_n1_unmatched[rrn_mask].copy()
            if "as_of_date" not in recent_rrn_clears.columns: recent_rrn_clears["as_of_date"] = pd.NaT
            recent_rrn_clears["cleared_using_stripe_date"] = pd.to_datetime(stripe_file_date)
            recent_rrn_clears["cleared_on_run_date"]       = pd.to_datetime(as_of_date)
            recent_rrn_clears.rename(columns={hist_rrn_col: "RRN"}, inplace=True)
            keys = ["RRN"] + (["match_key"] if "match_key" in recent_rrn_clears.columns else [])
            ss.rrn_match_history = pd.concat([ss.rrn_match_history, sanitize_for_excel(recent_rrn_clears)], ignore_index=True)
            ss.rrn_match_history = ss.rrn_match_history.drop_duplicates(subset=keys, keep="last")
            ss.hist_n1_unmatched = ss.hist_n1_unmatched[~rrn_mask].copy()
            ss.recent_rrn_clears = recent_rrn_clears
        else:
            ss.recent_rrn_clears = pd.DataFrame()

        # Update cumulative unmatched history
        def add_as_of(df):
            if df.empty: return df
            out = df.copy(); out["as_of_date"] = pd.to_datetime(as_of_date); return out

        upd_n1_hist     = pd.concat([ss.hist_n1_unmatched,     add_as_of(n1_unmatched)],     ignore_index=True)
        upd_stripe_hist = pd.concat([ss.hist_stripe_unmatched, add_as_of(stripe_unmatched)], ignore_index=True)

        if "match_key" in upd_n1_hist.columns:
            upd_n1_hist = upd_n1_hist.drop_duplicates(subset="match_key", keep="first")
        else:
            upd_n1_hist = upd_n1_hist.drop_duplicates(keep="first")
        if "stripe_match_key" in upd_stripe_hist.columns:
            upd_stripe_hist = upd_stripe_hist.drop_duplicates(subset="stripe_match_key", keep="first")
        else:
            upd_stripe_hist = upd_stripe_hist.drop_duplicates(keep="first")

        ss.hist_n1_unmatched   = sanitize_for_excel(upd_n1_hist)
        ss.hist_stripe_unmatched = sanitize_for_excel(upd_stripe_hist)

        # Summary metrics
        n1_total        = len(n1_df);          stripe_total    = len(stripe_df)
        n1_matched_amt  = n1_matched.get("effective_auth_amount", pd.Series([], dtype=float)).sum()
        stripe_matched_amt = stripe_matched.get("net", pd.Series([], dtype=float)).sum()
        n1_recon_pct    = round((len(n1_matched)     / n1_total)     * 100, 2) if n1_total    else 0
        stripe_recon_pct= round((len(stripe_matched) / stripe_total) * 100, 2) if stripe_total else 0

        summary_df = pd.DataFrame({
            "Source":           ["N1","Stripe"],
            "Matched Count":    [len(n1_matched),     len(stripe_matched)],
            "Matched Amount":   [n1_matched_amt,       stripe_matched_amt],
            "Unmatched Count":  [len(n1_unmatched),   len(stripe_unmatched)],
            "Total":            [n1_total,             stripe_total],
            "Reconciliation %": [n1_recon_pct,         stripe_recon_pct],
        })

        # Append to daily log
        ss.recon_log_df = append_daily_log(
            ss.recon_log_df, as_of_date, n1_file,
            n1_total, len(n1_matched), n1_matched_amt, len(n1_unmatched), n1_recon_pct,
            stripe_total, len(stripe_matched), stripe_matched_amt, len(stripe_unmatched), stripe_recon_pct
        )

        # =========================================================
        # SAVE ALL DATA BACK TO GITHUB
        # This is the key step — persists data so tomorrow's session
        # restores today's records automatically.
        # =========================================================
        with st.spinner("💾 Saving data to GitHub for persistence..."):
            save_results = save_all_to_github()
            failed = [k for k, v in save_results.items() if not v]
            if failed:
                st.warning(f"⚠️ Could not save to GitHub: {', '.join(failed)}. Check your GITHUB_TOKEN secret.")
            else:
                st.success("✅ Data saved to GitHub — records will persist across sessions.")

else:
    st.info("Please upload N1, Stripe, and PI_Numbers files to begin reconciliation.")

# ===================== KPI Tiles ================================
def kpi_tile(label, value, help_text=None, key=None, target_filter=None):
    c = st.container()
    col1, col2 = c.columns([1,1])
    with col1:
        st.metric(label, value, help=help_text)
    with col2:
        if target_filter and st.button("View", key=key):
            if target_filter == "unmatched_only":
                st.toast("Applied focus: unmatched tables below", icon="🔎")
            elif target_filter == "refunds_only":
                ss.global_filters["category"] = ["refund","refund_failure"]
                st.toast("Applied filter: refunds", icon="🔎")

if not summary_df.empty:
    c1, c2, c3, c4 = st.columns(4)
    with c1: kpi_tile("N1 Recon %",       f"{n1_recon_pct:.2f}%",    key="k1")
    with c2: kpi_tile("Stripe Recon %",   f"{stripe_recon_pct:.2f}%",key="k2")
    with c3: kpi_tile("N1 Unmatched",     f"{len(n1_unmatched)}",    key="k3", target_filter="unmatched_only")
    with c4: kpi_tile("Stripe Unmatched", f"{len(stripe_unmatched)}",key="k4", target_filter="unmatched_only")

# ===================== Summary + Pie Charts =====================
if not summary_df.empty:
    st.subheader("📊 Reconciliation Summary")
    st.dataframe(summary_df, use_container_width=True)

    n1_unmatched_v   = apply_global_filters(n1_unmatched,   "N1")
    stripe_unmatched_v = apply_global_filters(stripe_unmatched, "Stripe")
    n1_matched_v     = apply_global_filters(n1_matched,     "N1")
    stripe_matched_v = apply_global_filters(stripe_matched, "Stripe")

    n1_m  = len(n1_matched_v);    n1_u  = len(n1_unmatched_v)
    stp_m = len(stripe_matched_v); stp_u = len(stripe_unmatched_v)
    n1_tot  = n1_m  + n1_u;       stp_tot = stp_m + stp_u
    n1_pct_v  = round((n1_m  / n1_tot)  * 100, 2) if n1_tot  else 0.0
    stp_pct_v = round((stp_m / stp_tot) * 100, 2) if stp_tot else 0.0

    labels = ["Matched","Unmatched"]
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    ax[0].pie([n1_m,  n1_u],  labels=labels, autopct="%1.2f%%", startangle=90, colors=["#2ca02c","#ff7f0e"])
    ax[0].set_title("N1 Match Distribution")
    ax[1].pie([stp_m, stp_u], labels=labels, autopct="%1.2f%%", startangle=90, colors=["#1f77b4","#ff7f0e"])
    ax[1].set_title("Stripe Match Distribution")
    st.pyplot(fig)

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown(f"**N1 Reconciliation:** {n1_pct_v:.2f}%  &nbsp;|&nbsp;  **Matched:** {n1_m} / **Total:** {n1_tot}")
    with col_b:
        st.markdown(f"**Stripe Reconciliation:** {stp_pct_v:.2f}%  &nbsp;|&nbsp;  **Matched:** {stp_m} / **Total:** {stp_tot}")

# ===================== Tabs ================================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
    ["✅ Matched Records", "❌ Unmatched Records", "🗂️ Unmatched History", "📅 Daily Trend", "📦 Run History", "🤖 Assistant"]
)

# ---------- Tab 1: Matched ----------
with tab1:
    st.subheader("Matched Records")
    st.write("N1 Matched (global filters applied for view only)")
    st.dataframe(apply_global_filters(n1_matched, "N1") if not n1_matched.empty else pd.DataFrame(), use_container_width=True)
    st.write("Stripe Matched (global filters applied for view only)")
    st.dataframe(apply_global_filters(stripe_matched, "Stripe") if not stripe_matched.empty else pd.DataFrame(), use_container_width=True)

# ---------- Tab 2: Unmatched (Editable) ----------
def capture_remarks_edits(original_df, edited_df, dataset, key_col_opts):
    diffs = []
    if original_df is None or original_df.empty or edited_df is None or edited_df.empty:
        return diffs
    rec_key = None
    for k in key_col_opts:
        if k in original_df.columns and k in edited_df.columns:
            rec_key = k; break
    if rec_key is None:
        rec_key = original_df.columns[0]
    merged  = original_df[[rec_key,"remarks"]].merge(
        edited_df[[rec_key,"remarks"]], on=rec_key, how="inner", suffixes=("_old","_new"))
    changed = merged[merged["remarks_old"].astype(str) != merged["remarks_new"].astype(str)]
    for _, r in changed.iterrows():
        diffs.append((
            extract_date_from_filename(n1_file) or str(as_of_date),
            dataset, str(r[rec_key]), "remarks",
            str(r["remarks_old"]), str(r["remarks_new"])
        ))
    return diffs

with tab2:
    st.subheader("Unmatched Records (Editable)")
    st.info("You can update remarks for 'Need to Investigate' records below.")
    st.markdown("### N1 Unmatched (filtered for view only)")
    n1_unmatched_view     = apply_global_filters(n1_unmatched,     "N1")
    edited_n1_unmatched   = st.data_editor(n1_unmatched_view,     num_rows="dynamic") if not n1_unmatched_view.empty     else pd.DataFrame()

    st.markdown("### Stripe Unmatched (filtered for view only)")
    stripe_unmatched_view   = apply_global_filters(stripe_unmatched, "Stripe")
    edited_stripe_unmatched = st.data_editor(stripe_unmatched_view, num_rows="dynamic") if not stripe_unmatched_view.empty else pd.DataFrame()

    cA, cB = st.columns(2)
    with cA:
        if not edited_n1_unmatched.empty:
            st.download_button("⬇️ Download N1 Unmatched (Edited View)",
                               convert_df_to_csv_bytes(edited_n1_unmatched),
                               "n1_unmatched_view.csv", "text/csv")
    with cB:
        if not edited_stripe_unmatched.empty:
            st.download_button("⬇️ Download Stripe Unmatched (Edited View)",
                               convert_df_to_csv_bytes(edited_stripe_unmatched),
                               "stripe_unmatched_view.csv", "text/csv")

    # Log remark edits to audit trail and save to GitHub
    if st.button("💾 Log remark edits & save to GitHub"):
        diffs = []
        diffs += capture_remarks_edits(n1_unmatched_view,     edited_n1_unmatched,   "N1",     ["match_key","payment_reference_number"])
        diffs += capture_remarks_edits(stripe_unmatched_view, edited_stripe_unmatched,"Stripe", ["stripe_match_key","payment_intent_id","source_id"])
        if diffs:
            run_id = extract_date_from_filename(n1_file) or str(as_of_date)
            new_rows = pd.DataFrame(diffs, columns=["run_id","dataset","record_key","field","old_value","new_value"])
            new_rows["ts"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
            ss.audit_trail_df = pd.concat([ss.audit_trail_df, new_rows], ignore_index=True)
            github_write_excel(GH_PATHS["audit_trail"], {"Audit_Trail": ss.audit_trail_df})
            st.success(f"✅ Logged {len(diffs)} remark change(s) and saved audit trail to GitHub.")
        else:
            st.info("No remark changes detected.")

# ---------- Tab 3: Unmatched History + Tools ----------
with tab3:
    st.subheader("🗂️ Unmatched History (Cumulative)")
    st.caption("Records persist across days via GitHub. Previous day records are automatically restored on startup.")
    c1, c2 = st.columns(2)
    with c1: show_n1     = st.checkbox("Show N1 Unmatched History",     value=True)
    with c2: show_stripe = st.checkbox("Show Stripe Unmatched History", value=True)

    if show_n1:
        st.markdown("### N1 Unmatched History")
        st.dataframe(ss.hist_n1_unmatched, use_container_width=True, height=300)
    if show_stripe:
        st.markdown("### Stripe Unmatched History")
        st.dataframe(ss.hist_stripe_unmatched, use_container_width=True, height=300)

    st.markdown("### ✅ RRN Match History (All-time)")
    if ss.rrn_match_history.empty:
        st.info("No RRN matches logged yet.")
    else:
        rrn_hist_sorted = ss.rrn_match_history.copy()
        for c in ["as_of_date","cleared_using_stripe_date","cleared_on_run_date"]:
            if c in rrn_hist_sorted.columns:
                rrn_hist_sorted[c] = pd.to_datetime(rrn_hist_sorted[c], errors="coerce")
            else:
                rrn_hist_sorted[c] = pd.NaT  # add missing column so sort never raises KeyError
        rrn_hist_sorted = rrn_hist_sorted.sort_values(
            by=["cleared_on_run_date","cleared_using_stripe_date","as_of_date"],
            na_position="last"
        )
        st.dataframe(rrn_hist_sorted, use_container_width=True, height=260)
        st.download_button("⬇️ Download RRN_Match_History.xlsx",
                           to_excel_bytes({"RRN_Match_History": rrn_hist_sorted}),
                           "RRN_Match_History.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if not (ss.hist_n1_unmatched.empty and ss.hist_stripe_unmatched.empty):
        st.download_button("⬇️ Download Updated Unmatched_History.xlsx",
                           to_excel_bytes({"N1_Unmatched": ss.hist_n1_unmatched, "Stripe_Unmatched": ss.hist_stripe_unmatched}),
                           "Unmatched_History.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # GitHub Tools
    st.markdown("### 🐙 GitHub Data Tools")
    col_gh1, col_gh2, col_gh3 = st.columns(3)
    with col_gh1:
        if st.button("🔄 Force re-save all data to GitHub"):
            with st.spinner("Saving..."):
                res = save_all_to_github()
                failed = [k for k, v in res.items() if not v]
                if failed:
                    st.error(f"Failed: {', '.join(failed)}")
                else:
                    st.success("✅ All data saved to GitHub successfully.")
    with col_gh2:
        if st.button("🔃 Reload data from GitHub"):
            del ss["github_loaded"]   # force re-load on next rerun
            st.rerun()
    with col_gh3:
        if not ss.audit_trail_df.empty:
            st.download_button("⬇️ Download Audit Trail",
                               to_excel_bytes({"Audit_Trail": ss.audit_trail_df}),
                               "Audit_Trail.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # Combined daily workbook download
    if not summary_df.empty:
        run_id_date = extract_date_from_filename(n1_file) or str(as_of_date)
        result_bytes = to_excel_bytes(
            {
                "N1_Matched":          n1_matched,
                "N1_Unmatched":        n1_unmatched,
                "Stripe_Matched":      stripe_matched,
                "Stripe_Unmatched":    stripe_unmatched,
                "Reconciliation_Log":  ss.recon_log_df,
                "RRN_Match_History":   ss.rrn_match_history,
            },
            dashboard_sheet_name="Dashboard",
            dashboard_df=summary_df.copy(),
        )
        st.download_button(
            f"⬇️ Download Combined_Comparison_Result_{run_id_date}.xlsx",
            result_bytes,
            f"Combined_Comparison_Result_{run_id_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ---------- Tab 4: Daily Trend ----------
with tab4:
    st.subheader("📅 Daily Trend")
    if ss.recon_log_df.empty:
        st.info("No daily log yet. Upload files and run at least once to populate the trend.")
    else:
        trend_df = ss.recon_log_df.copy()
        trend_df["Date"] = pd.to_datetime(trend_df["Date"])
        trend_df = trend_df.sort_values("Date")
        cA, cB = st.columns(2)
        chart_type = cA.radio("Chart type", ["Line","Bar"], horizontal=True)
        metric     = cB.selectbox("Metric", ["Reconciliation %","Matched Count","Unmatched Count","Matched Amount"], index=0)
        metric_map = {
            "Reconciliation %": ("N1 Reconciliation %",  "Stripe Reconciliation %"),
            "Matched Count":    ("N1 Matched Count",      "Stripe Matched Count"),
            "Unmatched Count":  ("N1 Unmatched Count",    "Stripe Unmatched Count"),
            "Matched Amount":   ("N1 Matched Amount",     "Stripe Matched Amount"),
        }
        n1_col, stp_col = metric_map[metric]
        fig, ax = plt.subplots(figsize=(10, 4.5))
        x     = trend_df["Date"]
        n1_y  = trend_df.get(n1_col,  pd.Series([0]*len(trend_df)))
        stp_y = trend_df.get(stp_col, pd.Series([0]*len(trend_df)))
        if chart_type == "Line":
            ax.plot(x, n1_y,  marker="o", color="tab:green", label=f"N1 {metric}")
            ax.plot(x, stp_y, marker="o", color="tab:blue",  label=f"Stripe {metric}")
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            fig.autofmt_xdate()
        else:
            idx   = np.arange(len(x)); width = 0.42
            ax.bar(idx - width/2, n1_y,  width, color="tab:green", label=f"N1 {metric}")
            ax.bar(idx + width/2, stp_y, width, color="tab:blue",  label=f"Stripe {metric}")
            ax.set_xticks(idx)
            ax.set_xticklabels([d.strftime("%Y-%m-%d") for d in x], rotation=45, ha="right")
        ax.set_title(f"Daily Trend — {metric}"); ax.set_xlabel("Date"); ax.set_ylabel(metric)
        ax.grid(True, alpha=0.25); ax.legend(loc="best")
        st.pyplot(fig)
        with st.expander("View Daily Reconciliation Log"):
            st.dataframe(trend_df, use_container_width=True, height=260)
        st.download_button("⬇️ Download Daily_Reconciliation_Log.xlsx",
                           to_excel_bytes({"Reconciliation_Log": trend_df}),
                           "Daily_Reconciliation_Log.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- Tab 5: Run History ----------
with tab5:
    st.subheader("📦 Previous Reconciliations (This Session)")
    if n1_file and stripe_file and pi_file and not summary_df.empty:
        derived_id = extract_date_from_filename(n1_file)
        if not derived_id:
            st.warning("Could not find date in N1 filename. Please rename like N1_YYYY-MM-DD.*")
            derived_id = str(as_of_date)
        ss.recon_history[derived_id] = {
            "as_of_date":       derived_id,
            "summary":          summary_df.copy(),
            "n1_unmatched":     n1_unmatched.copy(),
            "stripe_unmatched": stripe_unmatched.copy(),
        }
    if ss.recon_history:
        for key in sorted(ss.recon_history.keys(), key=lambda k: pd.to_datetime(k, errors="coerce")):
            entry = ss.recon_history[key]
            st.markdown(f"**Run ID:** {key} • **Date:** {entry['as_of_date']}")
            st.dataframe(entry["summary"], use_container_width=True)
    else:
        st.info("No previous reconciliations saved yet.")

# ---------- Tab 6: Assistant ----------
with tab6:
    st.subheader("🤖 Assistant")
    st.markdown("Ask something like: 'Show Stripe unmatched refunds above 1000 last 7 days', 'Top 5 merchants by unmatched count'.")
    user_input = st.text_input("Your question")

    def get_col(df, options, contains=False): return find_first_matching_col(df, options, contains=contains)
    n1_date_col     = get_col(n1_df,     ["date","transaction_date","created_at","created","as_of_date"], contains=True)
    stripe_date_col = get_col(stripe_df, ["date","transaction_date","created_at","created","created_utc"],  contains=True)
    n1_amount_col   = get_col(n1_df,     ["amount","effective_auth_amount"], contains=True) or "effective_auth_amount"
    stripe_amount_col = get_col(stripe_df, ["amount","net"], contains=True) or "net"
    n1_merchant_col = get_col(n1_df,     ["merchant","merchant_name","merchant id","merchant_id"], contains=True)
    stripe_merchant_col = get_col(stripe_df, ["merchant","merchant_name","merchant id","merchant_id","statement_descriptor"], contains=True)

    def normalize_text(s): return re.sub(r"\s+"," ", s.lower()).strip()
    def parse_range(text):
        m = re.search(r"between\s+([\d\.]+)\s+and\s+([\d\.]+)", text)
        if m: return float(m.group(1)), float(m.group(2))
        m = re.search(r">\s*([\d\.]+)", text)
        if m: return float(m.group(1)), None
        m = re.search(r"<\s*([\d\.]+)", text)
        if m: return None, float(m.group(1))
        return None, None
    def parse_last_days(text):
        m = re.search(r"last\s+(\d+)\s+days", text)
        return int(m.group(1)) if m else None
    def filter_by_amount(df, col, q):
        if col and col in df.columns:
            low, high = parse_range(q)
            vals = pd.to_numeric(df[col], errors="coerce")
            if low is not None and high is not None: return df[vals.between(low, high)]
            if low is not None:  return df[vals > low]
            if high is not None: return df[vals < high]
        return df
    def filter_by_last_days(df, date_col, q):
        if date_col and date_col in df.columns:
            try:
                dt   = pd.to_datetime(df[date_col], errors="coerce")
                days = parse_last_days(q)
                if days:
                    cutoff = pd.Timestamp.today() - pd.Timedelta(days=days)
                    return df[dt >= cutoff]
            except Exception: pass
        return df
    def filter_by_keyword(df, col, q):
        m = re.search(r"(merchant|descriptor|name)\s*:\s*([^\;,\|]+)", q)
        if col and m:
            kw = m.group(2).strip().lower()
            return df[df[col].astype(str).str.lower().str.contains(kw, na=False)]
        return df
    def quick_agg(df, scope): return f"There are {len(df)} {scope} items."

    def assistant_answer(question):
        q = normalize_text(question)
        if not q: return "Ask something to start."
        is_unmatched  = "unmatched" in q or "not matched" in q
        is_matched    = ("matched" in q or "match" in q) and not is_unmatched
        want_n1       = "n1" in q;    want_stripe = "stripe" in q

        src_unmatched = n1_unmatched     if want_n1 else (stripe_unmatched     if want_stripe else None)
        src_matched   = n1_matched       if want_n1 else (stripe_matched       if want_stripe else None)
        src_all       = n1_df            if want_n1 else (stripe_df            if want_stripe else None)

        if want_n1:
            df_amount_col, df_date_col, df_merch_col = n1_amount_col,     n1_date_col,     n1_merchant_col
        else:
            df_amount_col, df_date_col, df_merch_col = stripe_amount_col, stripe_date_col, stripe_merchant_col

        if "reconciliation" in q:
            if want_n1:     return f"N1 reconciliation percentage is {n1_recon_pct:.2f}%."
            if want_stripe: return f"Stripe reconciliation percentage is {stripe_recon_pct:.2f}%."
            return f"N1 reconciliation: {n1_recon_pct:.2f}% • Stripe reconciliation: {stripe_recon_pct:.2f}%."
        if ("total" in q or "count" in q) and not (is_unmatched or is_matched):
            if want_n1:     return f"N1 total transactions: {n1_total}"
            if want_stripe: return f"Stripe total transactions: {stripe_total}"
            return f"N1 total: {n1_total} • Stripe total: {stripe_total}"
        if "matched amount" in q or "amount matched" in q:
            if want_n1:     return f"Matched amount in N1 is {n1_matched_amt:,.2f}"
            if want_stripe: return f"Matched amount in Stripe is {stripe_matched_amt:,.2f}"
            return f"N1 matched amount: {n1_matched_amt:,.2f} • Stripe matched amount: {stripe_matched_amt:,.2f}"

        show_table = "show" in q or "list" in q or "display" in q or "table" in q
        topn_m     = re.search(r"top\s*(\d+)", q)

        if is_unmatched and src_unmatched is not None:
            df = src_unmatched.copy(); scope = f"{('N1' if want_n1 else 'Stripe')} unmatched"
        elif is_matched and src_matched is not None:
            df = src_matched.copy();   scope = f"{('N1' if want_n1 else 'Stripe')} matched"
        elif src_all is not None:
            df = src_all.copy();       scope = f"{('N1' if want_n1 else 'Stripe')} all"
        else:
            return f"N1 unmatched: {len(n1_unmatched)} • Stripe unmatched: {len(stripe_unmatched)}"

        df = filter_by_amount(df,    df_amount_col, q)
        df = filter_by_last_days(df, df_date_col,   q)
        df = filter_by_keyword(df,   df_merch_col,  q)
        if "refund" in q or "chargeback" in q:
            if "reporting_category" in df.columns:
                df = df[df["reporting_category"].astype(str).str.lower().isin(["refund","refund_failure"])]
        if "charge" in q and "refund" not in q:
            if "reporting_category" in df.columns:
                df = df[df["reporting_category"].astype(str).str.lower() == "charge"]

        if topn_m:
            n = int(topn_m.group(1))
            if df_merch_col and df_merch_col in df.columns:
                agg = df.groupby(df_merch_col).size().sort_values(ascending=False).head(n)
                st.dataframe(agg.reset_index().rename(columns={0:"count"}), use_container_width=True)
                return f"Top {n} by count for {scope}."
            elif df_amount_col and df_amount_col in df.columns:
                topn = df.sort_values(by=df_amount_col, ascending=False).head(n)
                st.dataframe(topn, use_container_width=True)
                return f"Top {n} by amount for {scope}."

        if show_table:
            st.dataframe(df, use_container_width=True, height=300)
            st.download_button("⬇️ Download result (CSV)",
                               convert_df_to_csv_bytes(sanitize_for_excel(df)),
                               f"{scope.replace(' ','_')}.csv", "text/csv")
            return f"{quick_agg(df, scope)} (table displayed & downloadable)."
        return quick_agg(df, scope)

    if user_input:
        resp = assistant_answer(user_input)
        ss.chat_history.append(("You", user_input))
        ss.chat_history.append(("Assistant", resp))

    if ss.chat_history:
        st.markdown("### 💬 Conversation History")
        for speaker, msg in ss.chat_history:
            st.markdown(f"**{speaker}:** {msg}")
    else:
        st.info("Ask something to start the conversation.")
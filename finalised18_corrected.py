# finalised21.py
# --- Reconciliation Dashboard with Built-in SQLite persistence and UX upgrades ---
# - Robust uploads: csv/xlsx/xls (delimiter & encoding detection for CSV)
# - Excel sanitization to prevent openpyxl IllegalCharacterError
# - RRN Match History (persistent) + removal from N1 unmatched history
# - Summary table + PIE charts for N1 & Stripe + reconciliation stats
# - Daily Trend, Run History, and upgraded Natural-Language Assistant
# - NEW: SQLite DB persistence (unmatched, exceptions cleared, daily log, audit trail)
# - NEW: Global Filters (date/merchant/category/amount) applied across views
# - NEW: KPI tiles with quick filters; Data Quality Gate; DB Tools exporter
# - Backward-compatible wrappers: extract_date_from_n1_filename, append_daily_log_from_n1

import io, re, json, sqlite3
from datetime import date, datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd
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
n1_file = st.sidebar.file_uploader("Upload N1 File", type=["xlsx", "xls", "csv"])
stripe_file = st.sidebar.file_uploader("Upload Stripe File", type=["xlsx", "xls", "csv"])
pi_file = st.sidebar.file_uploader("Upload PI_Numbers File", type=["xlsx", "xls", "csv"])
history_file = st.sidebar.file_uploader("Upload Unmatched_History (optional)", type=["xlsx", "xls", "csv"])
log_file = st.sidebar.file_uploader("Upload Daily_Reconciliation_Log (optional)", type=["xlsx", "xls", "csv"])
rrn_history_file = st.sidebar.file_uploader("Upload RRN_Match_History (optional)", type=["xlsx", "xls", "csv"])

as_of_date = st.sidebar.date_input("As-of date for this run", value=date.today())

# =============== DB: Paths & Helpers ===================
DB_PATH = "recon_store.sqlite3"

def db_connect():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def ensure_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unmatched_records(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT, run_date TEXT, dataset TEXT, as_of_date TEXT,
            match_key TEXT, stripe_match_key TEXT, rrn TEXT,
            amount REAL, record_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS exceptions_cleared(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            cleared_on_run_date TEXT,
            cleared_using_stripe_date TEXT,
            original_as_of_date TEXT,
            dataset TEXT,
            match_key TEXT,
            rrn TEXT,
            amount REAL,
            record_json TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_recon_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            n1_total INTEGER, n1_matched_count INTEGER, n1_matched_amount REAL, n1_unmatched_count INTEGER, n1_recon_pct REAL,
            stripe_total INTEGER, stripe_matched_count INTEGER, stripe_matched_amount REAL, stripe_unmatched_count INTEGER, stripe_recon_pct REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit_trail(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT, dataset TEXT, record_key TEXT,
            field TEXT, old_value TEXT, new_value TEXT,
            ts TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

def insert_daily_log(conn, log_row: dict):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO daily_recon_log
        (date,n1_total,n1_matched_count,n1_matched_amount,n1_unmatched_count,n1_recon_pct,
         stripe_total,stripe_matched_count,stripe_matched_amount,stripe_unmatched_count,stripe_recon_pct)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (
        str(log_row["Date"].date()),
        log_row["N1 Total"], log_row["N1 Matched Count"], log_row["N1 Matched Amount"], log_row["N1 Unmatched Count"], log_row["N1 Reconciliation %"],
        log_row["Stripe Total"], log_row["Stripe Matched Count"], log_row["Stripe Matched Amount"], log_row["Stripe Unmatched Count"], log_row["Stripe Reconciliation %"],
    ))
    conn.commit()

def sanitize_value_for_json(v):
    if pd.isna(v):
        return None
    s = str(v)
    # Strip control characters to keep JSON/Excel safe
    s = re.sub(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]", "", s)
    return s

def df_rows_to_json(df):
    return df.apply(lambda r: json.dumps({k: sanitize_value_for_json(v) for k, v in r.items()}, ensure_ascii=False), axis=1)

def save_unmatched_to_db(conn, df: pd.DataFrame, dataset: str, run_id: str, run_dt: datetime, as_of_dt: date):
    if df is None or df.empty:
        return 0
    # Pick optional columns safely
    rrn_col = find_first_matching_col(df, ["retrieval_reference_number", "RRN", "retrieval reference number"])
    mk = df.get("match_key", pd.Series([None]*len(df)))
    smk = df.get("stripe_match_key", pd.Series([None]*len(df)))
    amt = (df["effective_auth_amount"] if dataset=="N1" and "effective_auth_amount" in df.columns
           else (df["net"] if "net" in df.columns else pd.Series([None]*len(df))))
    rrn = df[rrn_col] if rrn_col in (df.columns if rrn_col else []) else pd.Series([None]*len(df))

    # JSON snapshot
    rec_json = df_rows_to_json(df)

    to_insert = pd.DataFrame({
        "run_id": run_id,
        "run_date": str(run_dt.date()),
        "dataset": dataset,
        "as_of_date": str(as_of_dt),
        "match_key": mk.astype(str, errors="ignore"),
        "stripe_match_key": smk.astype(str, errors="ignore"),
        "rrn": rrn,
        "amount": pd.to_numeric(amt, errors="coerce"),
        "record_json": rec_json
    })
    to_insert = to_insert.where(pd.notnull(to_insert), None)

    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO unmatched_records
        (run_id,run_date,dataset,as_of_date,match_key,stripe_match_key,rrn,amount,record_json)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, list(map(tuple, to_insert.values)))
    conn.commit()
    return len(to_insert)

def save_exceptions_cleared_to_db(conn, df_recent_clears: pd.DataFrame, run_id: str, dataset="N1"):
    if df_recent_clears is None or df_recent_clears.empty:
        return 0
    mk = df_recent_clears.get("match_key", pd.Series([None]*len(df_recent_clears)))
    rrn = df_recent_clears.get("RRN", pd.Series([None]*len(df_recent_clears)))
    amt = df_recent_clears.get("effective_auth_amount", pd.Series([None]*len(df_recent_clears)))
    orig_as_of = df_recent_clears.get("as_of_date", pd.Series([None]*len(df_recent_clears)))
    cleared_using = df_recent_clears.get("cleared_using_stripe_date", pd.Series([None]*len(df_recent_clears)))
    cleared_on = df_recent_clears.get("cleared_on_run_date", pd.Series([None]*len(df_recent_clears)))

    rec_json = df_rows_to_json(df_recent_clears)
    to_insert = pd.DataFrame({
        "run_id": run_id,
        "cleared_on_run_date": pd.to_datetime(cleared_on).dt.date.astype(str),
        "cleared_using_stripe_date": pd.to_datetime(cleared_using).dt.date.astype(str),
        "original_as_of_date": pd.to_datetime(orig_as_of, errors="coerce").dt.date.astype(str),
        "dataset": dataset,
        "match_key": mk.astype(str, errors="ignore"),
        "rrn": rrn.astype(str, errors="ignore"),
        "amount": pd.to_numeric(amt, errors="coerce"),
        "record_json": rec_json
    }).where(pd.notnull, None)

    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO exceptions_cleared
        (run_id,cleared_on_run_date,cleared_using_stripe_date,original_as_of_date,dataset,match_key,rrn,amount,record_json)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, list(map(tuple, to_insert.values)))
    conn.commit()
    return len(to_insert)

def log_audit_trail(conn, run_id: str, dataset: str, diffs: list):
    if not diffs:
        return 0
    cur = conn.cursor()
    cur.executemany("""
        INSERT INTO audit_trail (run_id, dataset, record_key, field, old_value, new_value)
        VALUES (?,?,?,?,?,?)
    """, diffs)
    conn.commit()
    return len(diffs)

def query_df(conn, sql, params=None):
    return pd.read_sql_query(sql, conn, params=params or ())

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
    obj_cols = out.select_dtypes(include=["object", "string"]).columns
    for c in obj_cols:
        out[c] = out[c].apply(_sanitize_obj)
    return out

def safe_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/*?\[\]]', "_", str(name))
    return name[:31] if len(name) > 31 else name

def to_excel_bytes(sheets: dict, dashboard_sheet_name: str = None, dashboard_df: pd.DataFrame = None) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        if dashboard_sheet_name is not None and dashboard_df is not None:
            sanitize_for_excel(dashboard_df).to_excel(writer, sheet_name=safe_sheet_name(dashboard_sheet_name), index=False)
        for n, d in sheets.items():
            sanitize_for_excel(d).to_excel(writer, sheet_name=safe_sheet_name(n), index=False)
    bio.seek(0)
    return bio.read()

def convert_df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def _standardize_ymd(y, m, d) -> str:
    return f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"

def extract_date_from_filename(file_obj):
    name = getattr(file_obj, 'name', '')
    m = re.search(r'(20\d{2})-(\d{2})-(\d{2})', name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None

def extract_date_from_n1_filename(n1_streamlit_file) -> Optional[str]:
    return extract_date_from_filename(n1_streamlit_file)

def append_daily_log_from_n1(recon_log_df: pd.DataFrame, as_of_date_param, n1_file_obj,
                             n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                             stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct) -> pd.DataFrame:
    return append_daily_log(recon_log_df, as_of_date_param, n1_file_obj,
                            n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                            stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct)

def append_daily_log(recon_log_df: pd.DataFrame, as_of_date_param, n1_file_obj,
                     n1_total, n1_matched_count, n1_matched_amt, n1_unmatched_count, n1_recon_pct,
                     stripe_total, stripe_matched_count, stripe_matched_amt, stripe_unmatched_count, stripe_recon_pct) -> pd.DataFrame:
    file_date = extract_date_from_filename(n1_file_obj)
    date_for_log = pd.to_datetime(file_date) if file_date else pd.to_datetime(as_of_date_param)
    row = {
        "Date": date_for_log,
        "N1 Total": int(n1_total),
        "N1 Matched Count": int(n1_matched_count),
        "N1 Matched Amount": float(n1_matched_amt),
        "N1 Unmatched Count": int(n1_unmatched_count),
        "N1 Reconciliation %": float(n1_recon_pct),
        "Stripe Total": int(stripe_total),
        "Stripe Matched Count": int(stripe_matched_count),
        "Stripe Matched Amount": float(stripe_matched_amt),
        "Stripe Unmatched Count": int(stripe_unmatched_count),
        "Stripe Reconciliation %": float(stripe_recon_pct),
    }
    out = pd.concat([recon_log_df, pd.DataFrame([row])], ignore_index=True)
    out = out.sort_values("Date").drop_duplicates(subset=["Date"], keep="last")
    return out

# ===================== Session State ============================
ss = st.session_state
for k, v in {
    "recon_history": {},
    "hist_n1_unmatched": pd.DataFrame(),
    "hist_stripe_unmatched": pd.DataFrame(),
    "recent_rrn_clears": pd.DataFrame(),
    "rrn_match_history": pd.DataFrame(),
    "recon_log_df": pd.DataFrame(),
    "chat_history": [],
    "global_filters": {},
}.items():
    if k not in ss: ss[k] = v

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
    gf_date_from = st.date_input("From date (as_of / created)", value=None)
    gf_date_to = st.date_input("To date (as_of / created)", value=None)
    gf_merchant_kw = st.text_input("Merchant contains:")
    gf_category = st.multiselect("Category (Stripe reporting_category)", ["charge", "refund", "refund_failure"])
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
    gf = ss.global_filters or {}
    # Date filter: try common date columns
    date_cols = [c for c in ["Date","as_of_date","created","created_at","created_utc","transaction_date"] if c in out.columns]
    if date_cols:
        dtcol = date_cols[0]
        dts = pd.to_datetime(out[dtcol], errors="coerce")
        if gf.get("date_from"): out = out[dts >= pd.to_datetime(gf["date_from"])]
        if gf.get("date_to"):   out = out[dts <= pd.to_datetime(gf["date_to"]) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)]
    # Merchant filter
    merch_col = None
    for c in ["merchant","merchant_name","merchant id","merchant_id","statement_descriptor"]:
        if c in out.columns:
            merch_col = c; break
    if merch_col and gf.get("merchant_kw"):
        out = out[out[merch_col].astype(str).str.contains(gf["merchant_kw"], case=False, na=False)]
    # Category filter (Stripe)
    if "reporting_category" in out.columns and gf.get("category"):
        out = out[out["reporting_category"].astype(str).str.lower().isin(gf["category"])]
    # Amount filter
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
        # Load
        n1_df = load_file(n1_file); stripe_df = load_file(stripe_file); pi_df = load_file(pi_file)
        n1_df = sanitize_for_excel(n1_df); stripe_df = sanitize_for_excel(stripe_df); pi_df = sanitize_for_excel(pi_df)

        # Optional histories
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
        n1_df.columns = n1_df.columns.str.strip(); stripe_df.columns = stripe_df.columns.str.strip(); pi_df.columns = pi_df.columns.str.strip()

        # Filter Stripe categories
        if "reporting_category" in stripe_df.columns:
            stripe_df = stripe_df[stripe_df["reporting_category"].astype(str).str.lower().isin(["charge","refund","refund_failure"])]

        # Clean amounts
        if "effective_auth_amount" in n1_df.columns: n1_df["effective_auth_amount"] = clean_amount(n1_df["effective_auth_amount"])
        if "net" in stripe_df.columns: stripe_df["net"] = clean_amount(stripe_df["net"])
        if "effective_auth_amount" in pi_df.columns: pi_df["effective_auth_amount"] = clean_amount(pi_df["effective_auth_amount"])

        # Match keys
        if {"payment_reference_number","effective_auth_amount"}.issubset(n1_df.columns):
            n1_df["match_key"] = n1_df["payment_reference_number"].astype(str) + "_" + n1_df["effective_auth_amount"].astype(str)
        if {"payment_reference_number","effective_auth_amount"}.issubset(pi_df.columns):
            pi_df["match_key"] = pi_df["payment_reference_number"].astype(str) + "_" + pi_df["effective_auth_amount"].astype(str)
        if "payment_intent_id" in stripe_df.columns and "net" in stripe_df.columns:
            stripe_df["match_key_intent"] = stripe_df["payment_intent_id"].astype(str) + "_" + stripe_df["net"].astype(str)
        if "source_id" in stripe_df.columns and "net" in stripe_df.columns:
            stripe_df["match_key_source"] = stripe_df["source_id"].astype(str) + "_" + stripe_df["net"].astype(str)

        # N1 -> Stripe (OR)
        cond_intent = n1_df.get("match_key", pd.Series([], dtype=str)).isin(stripe_df.get("match_key_intent", pd.Series([], dtype=str)))
        cond_source = n1_df.get("match_key", pd.Series([], dtype=str)).isin(stripe_df.get("match_key_source", pd.Series([], dtype=str)))
        n1_matched_mask = cond_intent | cond_source
        n1_matched = n1_df[n1_matched_mask].copy()
        n1_unmatched = n1_df[~n1_matched_mask].copy()
        n1_matched["Matched_From"] = "Stripe"; n1_matched["remarks"] = "matched"
        if "status" in n1_unmatched.columns:
            n1_unmatched["remarks"] = n1_unmatched["status"].apply(lambda x: "Pending Transactions" if str(x).strip().upper() == "PENDING" else "Need to Investigate")
        else:
            n1_unmatched["remarks"] = "Need to Investigate"
        n1_unmatched["Matched_From"] = "N1"

        # Stripe -> (N1+PI)
        combined_keys = pd.concat([n1_df.get("match_key", pd.Series([], dtype=str)), pi_df.get("match_key", pd.Series([], dtype=str))], ignore_index=True)
        def stripe_match_key(row):
            cat = str(row.get("reporting_category","")).strip().lower()
            net_str = str(row.get("net","")); intent_id = str(row.get("payment_intent_id","")); source_id = str(row.get("source_id",""))
            if cat == "charge": return f"{intent_id}_{net_str}"
            elif cat in ["refund","refund_failure"]:
                source_key = f"{source_id}_{net_str}"; intent_key = f"{intent_id}_{net_str}"
                if source_key in combined_keys.values: return source_key
                if intent_key in combined_keys.values: return intent_key
                return None
            return None
        stripe_df["stripe_match_key"] = stripe_df.apply(stripe_match_key, axis=1)
        stripe_matched_mask = stripe_df["stripe_match_key"].isin(n1_df.get("match_key", pd.Series([], dtype=str)))
        stripe_matched = stripe_df[stripe_matched_mask].copy()
        stripe_unmatched = stripe_df[~stripe_matched_mask].copy().reset_index(drop=True)
        stripe_matched["Matched_From"] = "N1"; stripe_matched["remarks"] = "matched"
        pi_match_mask = stripe_unmatched["stripe_match_key"].isin(pi_df.get("match_key", pd.Series([], dtype=str)))
        stripe_unmatched["Matched_From"] = "Stripe"; stripe_unmatched["remarks"] = ""
        stripe_unmatched.loc[pi_match_mask, "Matched_From"] = "PI_Numbers"
        stripe_unmatched.loc[pi_match_mask, "remarks"] = "Dispute Write Off/Chargeback Credit"
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
        hist_rrn_col = find_first_matching_col(ss.hist_n1_unmatched, ["retrieval_reference_number", "RRN", "retrieval reference number"])
        stripe_rrn_col = (find_first_matching_col(stripe_df, ["payment_metadata[RRN]", "RRN", "rrn"], contains=True)
                          or find_first_matching_col(stripe_df, ["payment_metadata[RRN]", "RRN", "rrn"]))
        stripe_file_date = extract_date_from_filename(stripe_file) or str(as_of_date)

        if not ss.hist_n1_unmatched.empty and hist_rrn_col and stripe_rrn_col:
            rrn_mask = ss.hist_n1_unmatched[hist_rrn_col].astype(str).isin(stripe_df[stripe_rrn_col].astype(str))
            recent_rrn_clears = ss.hist_n1_unmatched[rrn_mask].copy()
            if "as_of_date" not in recent_rrn_clears.columns: recent_rrn_clears["as_of_date"] = pd.NaT
            recent_rrn_clears["cleared_using_stripe_date"] = pd.to_datetime(stripe_file_date)
            recent_rrn_clears["cleared_on_run_date"] = pd.to_datetime(as_of_date)
            recent_rrn_clears.rename(columns={hist_rrn_col: "RRN"}, inplace=True)
            keys = ["RRN"] + (["match_key"] if "match_key" in recent_rrn_clears.columns else [])
            ss.rrn_match_history = pd.concat([ss.rrn_match_history, sanitize_for_excel(recent_rrn_clears)], ignore_index=True)
            ss.rrn_match_history = ss.rrn_match_history.drop_duplicates(subset=keys, keep="last")
            ss.hist_n1_unmatched = ss.hist_n1_unmatched[~rrn_mask].copy()
            ss.recent_rrn_clears = recent_rrn_clears
        else:
            ss.recent_rrn_clears = pd.DataFrame()

        # Update unmatched history cumulative
        def add_as_of(df):
            if df.empty: return df
            out = df.copy(); out["as_of_date"] = pd.to_datetime(as_of_date); return out
        upd_n1_hist = pd.concat([ss.hist_n1_unmatched, add_as_of(n1_unmatched)], ignore_index=True)
        upd_stripe_hist = pd.concat([ss.hist_stripe_unmatched, add_as_of(stripe_unmatched)], ignore_index=True)
        if "match_key" in upd_n1_hist.columns: upd_n1_hist = upd_n1_hist.drop_duplicates(subset="match_key", keep="first")
        else: upd_n1_hist = upd_n1_hist.drop_duplicates(keep="first")
        if "stripe_match_key" in upd_stripe_hist.columns: upd_stripe_hist = upd_stripe_hist.drop_duplicates(subset="stripe_match_key", keep="first")
        else: upd_stripe_hist = upd_stripe_hist.drop_duplicates(keep="first")
        ss.hist_n1_unmatched = sanitize_for_excel(upd_n1_hist)
        ss.hist_stripe_unmatched = sanitize_for_excel(upd_stripe_hist)

        # Summary metrics
        n1_total, stripe_total = len(n1_df), len(stripe_df)
        n1_matched_amt = n1_matched.get("effective_auth_amount", pd.Series([], dtype=float)).sum()
        stripe_matched_amt = stripe_matched.get("net", pd.Series([], dtype=float)).sum()
        n1_recon_pct = round((len(n1_matched) / n1_total) * 100, 2) if n1_total else 0
        stripe_recon_pct = round((len(stripe_matched) / stripe_total) * 100, 2) if stripe_total else 0
        summary_df = pd.DataFrame({
            "Source": ["N1","Stripe"],
            "Matched Count": [len(n1_matched), len(stripe_matched)],
            "Matched Amount": [n1_matched_amt, stripe_matched_amt],
            "Unmatched Count": [len(n1_unmatched), len(stripe_unmatched)],
            "Total": [n1_total, stripe_total],
            "Reconciliation %": [n1_recon_pct, stripe_recon_pct],
        })

        # Append to daily log (memory)
        ss.recon_log_df = append_daily_log(ss.recon_log_df, as_of_date, n1_file, n1_total, len(n1_matched), n1_matched_amt,
                                           len(n1_unmatched), n1_recon_pct, stripe_total, len(stripe_matched),
                                           stripe_matched_amt, len(stripe_unmatched), stripe_recon_pct)

        # ------------------ PERSISTENCE: SQLite -------------------
        run_id = extract_date_from_filename(n1_file) or str(as_of_date)
        run_dt = pd.to_datetime(as_of_date)

        conn = db_connect(); ensure_tables(conn)

        # Daily log row → DB
        last_row = ss.recon_log_df[ss.recon_log_df["Date"] == ss.recon_log_df["Date"].max()].iloc[0].to_dict()
        insert_daily_log(conn, last_row)

        # Save unmatched snapshots → DB (after filters computed, but we save raw)
        save_unmatched_to_db(conn, n1_unmatched, "N1", run_id, run_dt, as_of_date)
        save_unmatched_to_db(conn, stripe_unmatched, "Stripe", run_id, run_dt, as_of_date)

        # Save cleared exceptions via RRN → DB
        save_exceptions_cleared_to_db(conn, ss.recent_rrn_clears, run_id, dataset="N1")

        conn.close()

else:
    st.info("Please upload N1, Stripe, and PI_Numbers files to begin reconciliation.")

# ===================== KPI Tiles & Quick Filters ================
def kpi_tile(label, value, help_text=None, key=None, target_filter=None):
    c = st.container()
    col1, col2 = c.columns([1,1])
    with col1:
        st.metric(label, value, help=help_text)
    with col2:
        if target_filter and st.button("View", key=key):
            # apply a quick filter into global filters; minimal: unmatched only, or refunds only
            if target_filter == "unmatched_only":
                ss.global_filters["min_amt"] = ss.global_filters.get("min_amt", 0.0)  # no-op just trigger re-run
                st.toast("Applied focus: unmatched tables below", icon="🔎")
            elif target_filter == "refunds_only":
                ss.global_filters["category"] = ["refund","refund_failure"]
                st.toast("Applied filter: refunds", icon="🔎")

if not summary_df.empty:
    c1, c2, c3, c4 = st.columns(4)
    with c1:  kpi_tile("N1 Recon %", f"{n1_recon_pct:.2f}%", key="k1")
    with c2:  kpi_tile("Stripe Recon %", f"{stripe_recon_pct:.2f}%", key="k2")
    with c3:  kpi_tile("N1 Unmatched", f"{len(n1_unmatched)}", key="k3", target_filter="unmatched_only")
    with c4:  kpi_tile("Stripe Unmatched", f"{len(stripe_unmatched)}", key="k4", target_filter="unmatched_only")

# ===================== Summary + Pie Charts =====================
if not summary_df.empty:
    st.subheader("📊 Reconciliation Summary")
    st.dataframe(summary_df, use_container_width=True)

    # Apply global filters to display pies consistently (only affects visuals, not saved data)
    n1_unmatched_v = apply_global_filters(n1_unmatched, "N1")
    stripe_unmatched_v = apply_global_filters(stripe_unmatched, "Stripe")
    n1_matched_v = apply_global_filters(n1_matched, "N1")
    stripe_matched_v = apply_global_filters(stripe_matched, "Stripe")

    n1_m, n1_u = len(n1_matched_v), len(n1_unmatched_v)
    stp_m, stp_u = len(stripe_matched_v), len(stripe_unmatched_v)

    n1_tot = n1_m + n1_u
    stp_tot = stp_m + stp_u
    n1_pct_v = round((n1_m/n1_tot)*100,2) if n1_tot else 0.0
    stp_pct_v = round((stp_m/stp_tot)*100,2) if stp_tot else 0.0

    labels = ["Matched","Unmatched"]
    fig, ax = plt.subplots(1, 2, figsize=(12, 4.5))
    ax[0].pie([n1_m, n1_u], labels=labels, autopct="%1.2f%%", startangle=90, colors=["#2ca02c","#ff7f0e"])
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
    # Key column detection
    rec_key = None
    for k in key_col_opts:
        if k in original_df.columns and k in edited_df.columns:
            rec_key = k; break
    if rec_key is None:
        rec_key = original_df.columns[0]  # fallback to first column positionally

    merged = original_df[[rec_key, "remarks"]].merge(
        edited_df[[rec_key, "remarks"]], on=rec_key, how="inner", suffixes=("_old","_new")
    )
    changed = merged[merged["remarks_old"].astype(str) != merged["remarks_new"].astype(str)]
    for _, r in changed.iterrows():
        diffs.append((extract_date_from_filename(n1_file) or str(as_of_date), dataset, str(r[rec_key]), "remarks",
                      str(r["remarks_old"]), str(r["remarks_new"])))
    return diffs

with tab2:
    st.subheader("Unmatched Records (Editable)")
    st.info("You can update remarks for 'Need to Investigate' records below.")
    st.markdown("### N1 Unmatched (filtered for view only)")
    n1_unmatched_view = apply_global_filters(n1_unmatched, "N1")
    edited_n1_unmatched = st.data_editor(n1_unmatched_view, num_rows="dynamic") if not n1_unmatched_view.empty else pd.DataFrame()

    st.markdown("### Stripe Unmatched (filtered for view only)")
    stripe_unmatched_view = apply_global_filters(stripe_unmatched, "Stripe")
    edited_stripe_unmatched = st.data_editor(stripe_unmatched_view, num_rows="dynamic") if not stripe_unmatched_view.empty else pd.DataFrame()

    # Download buttons (edited views)
    cA, cB = st.columns(2)
    with cA:
        if not edited_n1_unmatched.empty:
            st.download_button("⬇️ Download N1 Unmatched (Edited View)", convert_df_to_csv_bytes(edited_n1_unmatched),
                               "n1_unmatched_view.csv", "text/csv")
    with cB:
        if not edited_stripe_unmatched.empty:
            st.download_button("⬇️ Download Stripe Unmatched (Edited View)", convert_df_to_csv_bytes(edited_stripe_unmatched),
                               "stripe_unmatched_view.csv", "text/csv")

    # Audit trail of remarks edits
    if st.button("💾 Log remark edits to DB"):
        conn = db_connect(); ensure_tables(conn)
        diffs = []
        diffs += capture_remarks_edits(n1_unmatched_view, edited_n1_unmatched, "N1", ["match_key","payment_reference_number"])
        diffs += capture_remarks_edits(stripe_unmatched_view, edited_stripe_unmatched, "Stripe", ["stripe_match_key","payment_intent_id","source_id"])
        n_changes = log_audit_trail(conn, extract_date_from_filename(n1_file) or str(as_of_date), "mixed", diffs)
        conn.close()
        st.success(f"Logged {n_changes} remark change(s) into audit trail.")

# ---------- Tab 3: Unmatched History + DB Tools ----------
with tab3:
    st.subheader("🗂️ Unmatched History (Cumulative)")
    st.caption("Includes previously unmatched items after applying RRN clears.")
    c1, c2 = st.columns(2)
    with c1:
        show_n1 = st.checkbox("Show N1 Unmatched History", value=True)
    with c2:
        show_stripe = st.checkbox("Show Stripe Unmatched History", value=True)

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
        rrn_hist_sorted = rrn_hist_sorted.sort_values(by=["cleared_on_run_date","cleared_using_stripe_date","as_of_date"])
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

    # --- DB Tools ---
    st.markdown("### 🗄️ Database Tools")
    conn = db_connect(); ensure_tables(conn)
    db_counts = {
        "unmatched_records": query_df(conn, "SELECT COUNT(*) AS cnt FROM unmatched_records")["cnt"].iloc[0],
        "exceptions_cleared": query_df(conn, "SELECT COUNT(*) AS cnt FROM exceptions_cleared")["cnt"].iloc[0],
        "daily_recon_log": query_df(conn, "SELECT COUNT(*) AS cnt FROM daily_recon_log")["cnt"].iloc[0],
        "audit_trail": query_df(conn, "SELECT COUNT(*) AS cnt FROM audit_trail")["cnt"].iloc[0],
    }
    st.write(f"**Rows** — unmatched_records: {db_counts['unmatched_records']}, exceptions_cleared: {db_counts['exceptions_cleared']}, daily_recon_log: {db_counts['daily_recon_log']}, audit_trail: {db_counts['audit_trail']}")
    colX, colY = st.columns(2)
    with colX:
        if st.button("⬇️ Export DB (Excel workbook)"):
            dfs = {
                "unmatched_records": query_df(conn, "SELECT * FROM unmatched_records ORDER BY id DESC LIMIT 50000"),
                "exceptions_cleared": query_df(conn, "SELECT * FROM exceptions_cleared ORDER BY id DESC LIMIT 50000"),
                "daily_recon_log": query_df(conn, "SELECT * FROM daily_recon_log ORDER BY id DESC LIMIT 5000"),
                "audit_trail": query_df(conn, "SELECT * FROM audit_trail ORDER BY id DESC LIMIT 50000"),
            }
            st.download_button("Download recon_store.xlsx", to_excel_bytes(dfs), "recon_store.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with colY:
        if st.button("🔄 Refresh DB stats"):
            st.experimental_rerun()
    conn.close()

    # Combined daily workbook
    if not summary_df.empty:
        run_id_date = extract_date_from_filename(n1_file) or str(as_of_date)
        result_bytes = to_excel_bytes(
            {
                "N1_Matched": n1_matched,
                "N1_Unmatched": n1_unmatched,
                "Stripe_Matched": stripe_matched,
                "Stripe_Unmatched": stripe_unmatched,
                "Reconciliation_Log": ss.recon_log_df,
                "RRN_Match_History": ss.rrn_match_history,
            },
            dashboard_sheet_name="Dashboard",
            dashboard_df=summary_df.copy(),
        )
        st.download_button(f"⬇️ Download Combined_Comparison_Result_{run_id_date}.xlsx", result_bytes,
                           f"Combined_Comparison_Result_{run_id_date}.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------- Tab 4: Daily Trend ----------
with tab4:
    st.subheader("📅 Daily Trend")
    if ss.recon_log_df.empty:
        st.info("No daily log yet. Upload files and run at least once to populate the trend.")
    else:
        trend_df = ss.recon_log_df.copy()
        trend_df["Date"] = pd.to_datetime(trend_df["Date"]); trend_df = trend_df.sort_values("Date")
        cA, cB = st.columns(2)
        chart_type = cA.radio("Chart type", ["Line","Bar"], horizontal=True)
        metric = cB.selectbox("Metric", ["Reconciliation %","Matched Count","Unmatched Count","Matched Amount"], index=0)
        metric_map = {
            "Reconciliation %": ("N1 Reconciliation %", "Stripe Reconciliation %"),
            "Matched Count": ("N1 Matched Count", "Stripe Matched Count"),
            "Unmatched Count": ("N1 Unmatched Count", "Stripe Unmatched Count"),
            "Matched Amount": ("N1 Matched Amount", "Stripe Matched Amount"),
        }
        n1_col, stp_col = metric_map[metric]
        fig, ax = plt.subplots(figsize=(10,4.5))
        x = trend_df["Date"]; n1_y = trend_df.get(n1_col, pd.Series([0]*len(trend_df))); stp_y = trend_df.get(stp_col, pd.Series([0]*len(trend_df)))
        if chart_type=="Line":
            ax.plot(x, n1_y, marker="o", color="tab:green", label=f"N1 {metric}")
            ax.plot(x, stp_y, marker="o", color="tab:blue", label=f"Stripe {metric}")
            ax.xaxis.set_major_locator(mdates.DayLocator(interval=1)); ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d')); fig.autofmt_xdate()
        else:
            idx = np.arange(len(x)); width=0.42
            ax.bar(idx - width/2, n1_y, width, color="tab:green", label=f"N1 {metric}")
            ax.bar(idx + width/2, stp_y, width, color="tab:blue", label=f"Stripe {metric}")
            ax.set_xticks(idx); ax.set_xticklabels([d.strftime("%Y-%m-%d") for d in x], rotation=45, ha="right")
        ax.set_title(f"Daily Trend — {metric}"); ax.set_xlabel("Date"); ax.set_ylabel(metric); ax.grid(True, alpha=0.25); ax.legend(loc="best")
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
            "as_of_date": derived_id,
            "summary": summary_df.copy(),
            "n1_unmatched": n1_unmatched.copy(),
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
    st.markdown("Ask something like: ‘Show Stripe unmatched refunds above 1000 last 7 days’, ‘Top 5 merchants by unmatched count’.")
    user_input = st.text_input("Your question")

    def get_col(df, options, contains=False): return find_first_matching_col(df, options, contains=contains)
    n1_date_col = get_col(n1_df, ["date","transaction_date","created_at","created","as_of_date"], contains=True)
    stripe_date_col = get_col(stripe_df, ["date","transaction_date","created_at","created","created_utc"], contains=True)
    n1_amount_col = get_col(n1_df, ["amount","effective_auth_amount"], contains=True) or "effective_auth_amount"
    stripe_amount_col = get_col(stripe_df, ["amount","net"], contains=True) or "net"
    n1_merchant_col = get_col(n1_df, ["merchant","merchant_name","merchant id","merchant_id"], contains=True)
    stripe_merchant_col = get_col(stripe_df, ["merchant","merchant_name","merchant id","merchant_id","statement_descriptor"], contains=True)

    def normalize_text(s: str) -> str: return re.sub(r"\s+"," ", s.lower()).strip()
    def parse_range(text: str): 
        m = re.search(r"between\s+([\d\.]+)\s+and\s+([\d\.]+)", text); 
        if m: return float(m.group(1)), float(m.group(2))
        m = re.search(r">\s*([\d\.]+)", text); 
        if m: return float(m.group(1)), None
        m = re.search(r"<\s*([\d\.]+)", text); 
        if m: return None, float(m.group(1))
        return None, None
    def parse_last_days(text: str):
        m = re.search(r"last\s+(\d+)\s+days", text)
        return int(m.group(1)) if m else None

    def filter_by_amount(df, col, q):
        if col and col in df.columns:
            low, high = parse_range(q)
            vals = pd.to_numeric(df[col], errors="coerce")
            if low is not None and high is not None: return df[vals.between(low, high)]
            if low is not None: return df[vals > low]
            if high is not None: return df[vals < high]
        return df
    def filter_by_last_days(df, date_col, q):
        if date_col and date_col in df.columns:
            try:
                dt = pd.to_datetime(df[date_col], errors="coerce")
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

    def assistant_answer(question: str):
        q = normalize_text(question); 
        if not q: return "Ask something to start."
        is_unmatched = "unmatched" in q or "not matched" in q
        is_matched = ("matched" in q or "match" in q) and not is_unmatched
        want_n1 = "n1" in q; want_stripe = "stripe" in q

        src_unmatched = n1_unmatched if want_n1 else (stripe_unmatched if want_stripe else None)
        src_matched = n1_matched if want_n1 else (stripe_matched if want_stripe else None)
        src_all = n1_df if want_n1 else (stripe_df if want_stripe else None)

        if want_n1:
            df_amount_col, df_date_col, df_merch_col = n1_amount_col, n1_date_col, n1_merchant_col
        else:
            df_amount_col, df_date_col, df_merch_col = stripe_amount_col, stripe_date_col, stripe_merchant_col

        if "reconciliation" in q:
            if want_n1: return f"N1 reconciliation percentage is {n1_recon_pct:.2f}%."
            if want_stripe: return f"Stripe reconciliation percentage is {stripe_recon_pct:.2f}%."
            return f"N1 reconciliation: {n1_recon_pct:.2f}% • Stripe reconciliation: {stripe_recon_pct:.2f}%."

        if ("total" in q or "count" in q) and not (is_unmatched or is_matched):
            if want_n1: return f"N1 total transactions: {n1_total}"
            if want_stripe: return f"Stripe total transactions: {stripe_total}"
            return f"N1 total: {n1_total} • Stripe total: {stripe_total}"

        if ("matched amount" in q or "amount matched" in q):
            if want_n1: return f"Matched amount in N1 is {n1_matched_amt:,.2f}"
            if want_stripe: return f"Matched amount in Stripe is {stripe_matched_amt:,.2f}"
            return f"N1 matched amount: {n1_matched_amt:,.2f} • Stripe matched amount: {stripe_matched_amt:,.2f}"

        show_table = "show" in q or "list" in q or "display" in q or "table" in q
        topn_m = re.search(r"top\s*(\d+)", q)

        if is_unmatched and src_unmatched is not None:
            df = src_unmatched.copy(); scope = f"{('N1' if want_n1 else 'Stripe')} unmatched"
        elif is_matched and src_matched is not None:
            df = src_matched.copy(); scope = f"{('N1' if want_n1 else 'Stripe')} matched"
        elif src_all is not None:
            df = src_all.copy(); scope = f"{('N1' if want_n1 else 'Stripe')} all"
        else:
            return f"N1 unmatched: {len(n1_unmatched)} • Stripe unmatched: {len(stripe_unmatched)}"

        df = filter_by_amount(df, df_amount_col, q)
        df = filter_by_last_days(df, df_date_col, q)
        df = filter_by_keyword(df, df_merch_col, q)
        if "refund" in q or "chargeback" in q:
            if "reporting_category" in df.columns:
                df = df[df["reporting_category"].astype(str).str.lower().isin(["refund","refund_failure"])]
        if "charge" in q and "refund" not in q:
            if "reporting_category" in df.columns:
                df = df[df["reporting_category"].astype(str).str.lower()=="charge"]

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
            st.download_button("⬇️ Download result (CSV)", convert_df_to_csv_bytes(sanitize_for_excel(df)),
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

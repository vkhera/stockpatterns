import os
import re
import glob
import requests
import pandas as pd
from datetime import datetime
# Additions: session with retries and logging
import logging
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

MTUM_URL = "https://www.ishares.com/us/products/251614/ishares-msci-usa-momentum-factor-etf/1467271812596.ajax?fileType=csv&fileName=MTUM_holdings&dataType=fund"

def make_session_with_retries(total_retries=5, backoff_factor=0.5, status_forcelist=(500,502,503,504)):
    session = requests.Session()
    retries = Retry(total=total_retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist, allowed_methods=["GET","POST"])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "fetch-mtum/1.0"})
    return session

def download_mtum_csv(session=None):
    sess = session or make_session_with_retries()
    try:
        resp = sess.get(MTUM_URL, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logging.warning(f"MTUM download failed: {e}")
        return None
    # try to get filename from header, else build one
    cd = resp.headers.get("Content-Disposition", "")
    filename = None
    if "filename=" in cd:
        m = re.findall(r'filename="?([^";]+)"?', cd)
        if m:
            filename = m[0]
    if not filename:
        filename = "MTUM_holdings.csv"
    today = datetime.now().strftime("%Y%m%d")
    out_name = filename
    # ensure date appended before extension
    base, ext = os.path.splitext(out_name)
    dated_name = f"{base}_{today}{ext}"
    path = os.path.join(OUTPUT_DIR, dated_name)
    with open(path, "wb") as f:
        f.write(resp.content)
    logging.info(f"Downloaded MTUM holdings to {path}")
    return path

def extract_ticker_from_text(s):
    if not isinstance(s, str):
        return None
    s = s.strip()
    # 1) look for ticker in parentheses e.g. "Apple Inc (AAPL)"
    m = re.search(r'\(([A-Z0-9\.\-]{1,8})\)', s)
    if m:
        return m.group(1).upper()
    # 2) look for tokens like "AAPL" (all caps, 1-6 letters/numbers)
    tokens = re.findall(r'\b[A-Z0-9]{1,6}\b', s)
    if tokens:
        # prefer last token (often ticker), but ensure not common words like 'US'
        for t in reversed(tokens):
            if not re.fullmatch(r'(US|USD|UNH|LLY|ETC|ETF)', t):  # basic filter
                return t.upper()
        return tokens[-1].upper()
    return None

def clean_mtum_df(raw_path):
    # iShares CSV has metadata in first 10 rows, actual header is at row 10 (0-indexed: row 9)
    if not raw_path:
        logging.warning("No raw file provided to clean_mtum_df")
        return None, None
    
    try:
        # Read CSV skipping first 10 rows, using row 10 as header
        df = pd.read_csv(raw_path, skiprows=10, dtype=str, encoding='utf-8', skip_blank_lines=True)
        
    except Exception as e:
        logging.error(f"Error reading MTUM CSV: {e}")
        return None, None
    
    # Look for a column that likely contains ticker
    cols_lower = [c.lower().strip() for c in df.columns]
    ticker_col = None
    for name in ("ticker", "symbol"):
        if name in cols_lower:
            ticker_col = df.columns[cols_lower.index(name)]
            break
    if ticker_col:
        df['Ticker'] = df[ticker_col].astype(str).str.strip().str.upper()
    else:
        # try to find a holding/name column
        name_col = None
        for name in ("holding", "holding name", "name", "security"):
            if name in cols_lower:
                name_col = df.columns[cols_lower.index(name)]
                break
        if name_col:
            df['Ticker'] = df[name_col].apply(extract_ticker_from_text)
        else:
            # try first column fallback
            first = df.columns[0]
            df['Ticker'] = df[first].apply(extract_ticker_from_text)
    # drop rows without tickers
    df = df[df['Ticker'].notnull() & df['Ticker'].str.strip().ne('')]
    df['Ticker'] = df['Ticker'].str.upper().str.strip()
    # produce a cleaned CSV with Ticker first
    cols = ['Ticker'] + [c for c in df.columns if c != 'Ticker']
    cleaned_path = os.path.splitext(raw_path)[0] + "_cleaned.csv"
    df.to_csv(cleaned_path, index=False, columns=cols, encoding='utf-8')
    print(f"Cleaned MTUM CSV saved to {cleaned_path}")
    return cleaned_path, df

def get_sorted_mtum_csvs():
    files = glob.glob(os.path.join(OUTPUT_DIR, "MTUM_holdings_*.csv"))
    # include cleaned variants too
    files += glob.glob(os.path.join(OUTPUT_DIR, "MTUM_holdings_*_cleaned.csv"))
    def extract_date(f):
        # try filename pattern to extract yyyymmdd
        bn = os.path.basename(f)
        m = re.search(r'(\d{8})', bn)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d")
            except Exception:
                pass
        return datetime.fromtimestamp(os.path.getmtime(f))
    files_sorted = sorted(set(files), key=extract_date)
    return files_sorted

def compare_latest_vs_previous():
    files = get_sorted_mtum_csvs()
    if not files:
        print("No MTUM files found in output/ to compare.")
        return
    # prefer cleaned files for comparison if present (choose latest cleaned if exists)
    # build list of base dates from filenames and pick latest two distinct dates
    dated = {}
    for f in files:
        m = re.search(r'(\d{8})', os.path.basename(f))
        if m:
            dated.setdefault(m.group(1), []).append(f)
    dates = sorted(dated.keys())
    if not dates:
        print("No dated MTUM files found.")
        return
    if len(dates) == 1:
        print("Only one dated MTUM file present:", dates[0])
        return
    latest_date = dates[-1]
    prev_date = dates[-2]
    # choose cleaned file if available, else raw
    def choose_file_for_date(d):
        candidates = dated.get(d, [])
        # prefer cleaned
        for c in candidates:
            if c.endswith("_cleaned.csv"):
                return c
        return sorted(candidates)[-1]
    latest_file = choose_file_for_date(latest_date)
    prev_file = choose_file_for_date(prev_date)
    print(f"Comparing MTUM {prev_date} -> {latest_date}")
    # read cleaned versions to extract Ticker
    df_latest = pd.read_csv(latest_file, dtype=str, encoding='utf-8')
    df_prev = pd.read_csv(prev_file, dtype=str, encoding='utf-8')
    # find Ticker column
    def get_tickers(df):
        cols = [c.lower() for c in df.columns]
        if 'ticker' in cols:
            return set(df[df.columns[cols.index('ticker')]].dropna().astype(str).str.upper().str.strip())
        # fallback: if first column header is 'Ticker' due to cleaned naming
        if df.columns[0].lower() == 'ticker':
            return set(df.iloc[:,0].dropna().astype(str).str.upper().str.strip())
        # last resort: try to find token-like values
        s = set()
        for col in df.columns:
            for v in df[col].dropna().astype(str):
                t = extract_ticker_from_text(v)
                if t:
                    s.add(t.upper())
        return s
    latest_tickers = get_tickers(df_latest)
    prev_tickers = get_tickers(df_prev)
    added = sorted(latest_tickers - prev_tickers)
    removed = sorted(prev_tickers - latest_tickers)
    if added:
        print("Additions:")
        for a in added:
            print(" ", a)
    else:
        print("Additions: None")
    if removed:
        print("Removals:")
        for r in removed:
            print(" ", r)
    else:
        print("Removals: None")
    # record to MTUM-Changes.txt
    record_changes_mtum(prev_file, latest_file, added, removed)

def record_changes_mtum(old_file, new_file, added, removed):
    path = os.path.join(OUTPUT_DIR, "ETF-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"--- {ts} | MTUM ETF | Compared: {os.path.basename(old_file)} -> {os.path.basename(new_file)} ---\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(header)
        if added:
            f.write("Additions:\n")
            for a in added:
                f.write(f"  {a}\n")
        else:
            f.write("Additions: None\n")
        if removed:
            f.write("Removals:\n")
            for r in removed:
                f.write(f"  {r}\n")
        else:
            f.write("Removals: None\n")
        f.write("\n")
    print(f"MTUM changes appended to {path}")

if __name__ == "__main__":
    try:
        raw = download_mtum_csv()
    except Exception as e:
        print("Error downloading MTUM CSV:", e)
    else:
        try:
            clean_mtum_df(raw)
        except Exception as e:
            print("Warning: cleaning MTUM CSV failed:", e)
        # compare latest two dated files
        compare_latest_vs_previous()

import os
import re
import glob
import requests
import pandas as pd
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

MMTM_URL = "https://www.ssga.com/us/en/intermediary/library-content/products/fund-data/etfs/us/holdings-daily-us-en-mmtm.xlsx"

def download_mmtm_xlsx():
    resp = requests.get(MMTM_URL, timeout=30)
    resp.raise_for_status()
    # derive filename and append date
    today = datetime.now().strftime("%Y%m%d")
    filename = f"holdings-daily-us-en-mmtm_{today}.xlsx"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "wb") as f:
        f.write(resp.content)
    print(f"Downloaded MMTM XLSX to {path}")
    return path

def find_ticker_column(df):
    # prefer obvious column names
    col_map = {c.lower(): c for c in df.columns}
    for candidate in ("ticker", "symbol", "ticker symbol", "fund ticker", "isin"):
        if candidate in col_map:
            return col_map[candidate]
    # otherwise choose the column with most ticker-like values (1-6 uppercase alnum)
    best_col = None
    best_score = -1
    pattern = re.compile(r'^[A-Z0-9\.\-]{1,6}$')
    for c in df.columns:
        s = df[c].dropna().astype(str).str.strip()
        if s.empty:
            continue
        # score fraction of values matching ticker-like pattern
        matches = s.str.match(pattern).sum()
        score = matches / len(s)
        if score > best_score:
            best_score = score
            best_col = c
    # require reasonable confidence
    if best_score >= 0.5:
        return best_col
    return None

def clean_mmtm_xlsx(xlsx_path):
    # try reading with header=0; if bad, fallback to header=None and detect header row
    try:
        df = pd.read_excel(xlsx_path, engine='openpyxl')
    except Exception:
        df = pd.read_excel(xlsx_path)
    # if no columns or small frame, try reading without header to detect header row
    if df.shape[1] == 0 or all(str(c).startswith("Unnamed") for c in df.columns):
        raw = pd.read_excel(xlsx_path, header=None)
        # find header row index containing 'Ticker' or 'Symbol' or 'Holding'
        header_row = None
        for i in range(min(10, len(raw))):
            row = raw.iloc[i].astype(str).str.lower().tolist()
            if any("ticker" in v or "symbol" in v or "holding" in v for v in row):
                header_row = i
                break
        if header_row is not None:
            df = pd.read_excel(xlsx_path, header=header_row, engine='openpyxl')
        else:
            # fallback to first sheet as-is
            df = pd.read_excel(xlsx_path, engine='openpyxl')
    # find ticker column
    ticker_col = find_ticker_column(df)
    if ticker_col is None:
        # attempt scanning all cells for ticker-like tokens and create Ticker column from first found
        tickers = []
        for _, row in df.iterrows():
            found = None
            for v in row.astype(str):
                if re.match(r'^[A-Z0-9\.\-]{1,6}$', v.strip()):
                    found = v.strip().upper()
                    break
            tickers.append(found)
        df['Ticker'] = tickers
    else:
        df['Ticker'] = df[ticker_col].astype(str).str.strip().str.upper().replace('nan', '')
    # drop rows without tickers
    df = df[df['Ticker'].notnull() & df['Ticker'].str.strip().ne('')]
    df['Ticker'] = df['Ticker'].str.upper().str.strip()
    # save cleaned CSV with date in name
    date_part = re.search(r'(\d{8})', os.path.basename(xlsx_path))
    date_str = date_part.group(1) if date_part else datetime.now().strftime("%Y%m%d")
    csv_name = f"MMTM_holdings_{date_str}.csv"
    csv_path = os.path.join(OUTPUT_DIR, csv_name)
    cols = ['Ticker'] + [c for c in df.columns if c != 'Ticker']
    df.to_csv(csv_path, index=False, columns=cols, encoding='utf-8')
    print(f"Cleaned MMTM holdings CSV saved to {csv_path}")
    return csv_path, df

def get_sorted_mmtm_csvs():
    files = glob.glob(os.path.join(OUTPUT_DIR, "MMTM_holdings_*.csv"))
    def extract_date(f):
        m = re.search(r'(\d{8})', os.path.basename(f))
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d")
            except Exception:
                pass
        return datetime.fromtimestamp(os.path.getmtime(f))
    return sorted(files, key=extract_date)

def compare_latest_previous_and_record():
    files = get_sorted_mmtm_csvs()
    if len(files) < 2:
        note = "Not enough MMTM CSV files to compare."
        print(note)
        record_etf_change(None, None, [], [], note)
        return
    latest = files[-1]
    prev = files[-2]
    print(f"Comparing MMTM: {os.path.basename(prev)} -> {os.path.basename(latest)}")
    df_latest = pd.read_csv(latest, dtype=str)
    df_prev = pd.read_csv(prev, dtype=str)
    latest_set = set(df_latest['Ticker'].dropna().astype(str).str.upper().str.strip())
    prev_set = set(df_prev['Ticker'].dropna().astype(str).str.upper().str.strip())
    added = sorted(latest_set - prev_set)
    removed = sorted(prev_set - latest_set)
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
    record_etf_change(prev, latest, added, removed)

def record_etf_change(old_file, new_file, added, removed, note=None):
    path = os.path.join(OUTPUT_DIR, "ETF-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"--- {ts} | Compared: {os.path.basename(old_file) if old_file else 'N/A'} -> {os.path.basename(new_file) if new_file else 'N/A'} ---\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(header)
        if note:
            f.write(f"{note}\n")
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
    print(f"Changes appended to {path}")

if __name__ == "__main__":
    try:
        xlsx = download_mmtm_xlsx()
    except Exception as e:
        print("Error downloading MMTM XLSX:", e)
    else:
        try:
            csv_path, df = clean_mmtm_xlsx(xlsx)
        except Exception as e:
            print("Error cleaning MMTM XLSX:", e)
        # compare latest two dated CSVs and record changes
        compare_latest_previous_and_record()

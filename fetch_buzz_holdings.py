import requests
import pandas as pd
import io
import re
import glob
import os
from datetime import datetime

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_buzz_holdings():
    excel_url = "https://www.vaneck.com/us/en/investments/social-sentiment-etf-buzz/downloads/holdings/"
    resp = requests.get(excel_url)
    resp.raise_for_status()
    # Try to get filename from Content-Disposition header
    cd = resp.headers.get("Content-Disposition", "")
    fname = None
    if "filename=" in cd:
        fname = re.findall('filename="?([^";]+)"?', cd)
        if fname:
            filename = fname[0]
        else:
            filename = "buzz_holdings.xlsx"
    else:
        filename = "buzz_holdings.xlsx"
    excel_path = os.path.join(OUTPUT_DIR, filename)
    # Save Excel file as-is in output folder
    with open(excel_path, "wb") as f:
        f.write(resp.content)
    print(f"BUZZ ETF holdings Excel saved to {excel_path}")
    # Also save as CSV with same base name in output folder
    df = pd.read_excel(io.BytesIO(resp.content))
    csv_filename = re.sub(r"\.xlsx?$", ".csv", filename, flags=re.IGNORECASE)
    csv_path = os.path.join(OUTPUT_DIR, csv_filename)
    df.to_csv(csv_path, index=False)
    print(f"BUZZ ETF holdings CSV saved to {csv_path}")

def get_sorted_buzz_csvs():
    files = glob.glob(os.path.join(OUTPUT_DIR, "BUZZ_asof_*.csv"))
    files = sorted(files, key=lambda x: x[-12:-4], reverse=True)
    return files

def clean_holdings_df(df, source_file):
    # Find the header row (row with 'Ticker' and 'Holding Name')
    header_row = None
    for i, row in df.iterrows():
        cols = [str(c).strip().lower() for c in row]
        if "ticker" in cols and ("holding name" in cols or "name" in cols):
            header_row = i
            break
    if header_row is not None:
        df = pd.read_csv(source_file, skiprows=header_row+1)
    # Remove cash/other rows and empty tickers
    df = df[df['Ticker'].notnull() & df['Ticker'].astype(str).str.strip().ne('')]
    df = df[~df['Ticker'].str.contains('CASH', na=False)]
    df = df[~df['Ticker'].str.contains('--', na=False)]
    df['Ticker'] = df['Ticker'].astype(str).str.strip()
    return df

def compare_holdings():
    files = get_sorted_buzz_csvs()
    if len(files) < 2:
        msg = "Not enough CSV files to compare."
        print(msg)
        record_etf_change(None, None, [], [], note=msg)
        return
    latest, previous = files[0], files[1]
    print(f"Comparing latest: {latest} with previous: {previous}")
    df_latest = pd.read_csv(latest, skip_blank_lines=True, dtype=str)
    df_prev = pd.read_csv(previous, skip_blank_lines=True, dtype=str)
    df_latest = clean_holdings_df(df_latest, latest)
    df_prev = clean_holdings_df(df_prev, previous)
    latest_tickers = set(df_latest['Ticker'])
    prev_tickers = set(df_prev['Ticker'])
    added = sorted(latest_tickers - prev_tickers)
    removed = sorted(prev_tickers - latest_tickers)
    if added:
        print("\nAdditions (new holdings in latest):")
        for t in added:
            print(f"  {t}")
    else:
        print("\nNo additions.")
    if removed:
        print("\nRemovals (holdings no longer present):")
        for t in removed:
            print(f"  {t}")
    else:
        print("\nNo removals.")
    
    # Record changes to ETF-Changes.txt
    record_etf_change(previous, latest, added, removed)

def record_etf_change(old_file, new_file, added, removed, note=None):
    """
    Append a timestamped entry to output/ETF-Changes.txt describing BUZZ ETF additions/removals.
    """
    path = os.path.join(OUTPUT_DIR, "ETF-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- {ts} | BUZZ ETF | Compared: {os.path.basename(old_file) if old_file else 'N/A'} -> {os.path.basename(new_file) if new_file else 'N/A'} ---\n")
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
    fetch_buzz_holdings()
    compare_holdings()

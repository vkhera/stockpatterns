import requests
import pandas as pd
import re
import glob
import os
from datetime import datetime, timedelta
import yfinance as yf

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

HDGE_URL = "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_HDGE_Holdings_File.csv"

def fetch_hdge_holdings():
    resp = requests.get(HDGE_URL, timeout=30)
    resp.raise_for_status()
    
    today = datetime.now().strftime("%Y%m%d")
    filename = f"HDGE_holdings_{today}.csv"
    csv_path = os.path.join(OUTPUT_DIR, filename)
    
    with open(csv_path, "wb") as f:
        f.write(resp.content)
    print(f"HDGE holdings CSV saved to {csv_path}")
    return csv_path

def get_sorted_hdge_csvs():
    files = glob.glob(os.path.join(OUTPUT_DIR, "HDGE_holdings_*.csv"))
    def extract_date(f):
        m = re.search(r'(\d{8})', os.path.basename(f))
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d")
            except Exception:
                pass
        return datetime.fromtimestamp(os.path.getmtime(f))
    return sorted(files, key=extract_date)

def clean_holdings_df(df):
    # HDGE CSV has ticker in column C (index 2)
    # First try to find by column name
    cols_lower = [c.lower() for c in df.columns]
    ticker_col = None
    for candidate in ("ticker", "symbol", "holding ticker", "fund ticker"):
        if candidate in cols_lower:
            ticker_col = df.columns[cols_lower.index(candidate)]
            break
    
    # If not found by name, use column C (index 2)
    if ticker_col is None:
        if len(df.columns) >= 3:
            ticker_col = df.columns[2]  # Column C (0-indexed: A=0, B=1, C=2)
        else:
            # Fallback to first column
            ticker_col = df.columns[0]
    
    df = df.rename(columns={ticker_col: 'Ticker'})
    df = df[df['Ticker'].notnull() & df['Ticker'].astype(str).str.strip().ne('')]
    df = df[~df['Ticker'].str.contains('CASH', na=False, case=False)]
    df = df[~df['Ticker'].str.contains('--', na=False)]
    df['Ticker'] = df['Ticker'].astype(str).str.strip().str.upper()
    return df

def check_price_increases(tickers):
    """
    Check which tickers have current price higher than 1 year ago.
    Returns list of tickers with price increases.
    """
    now = datetime.now()
    one_year_ago = now - timedelta(days=365)
    
    higher_now = []
    
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=one_year_ago, end=now)
            
            if hist.empty or len(hist) < 2:
                continue
            
            price_1y_ago = hist['Close'].iloc[0]
            current_price = hist['Close'].iloc[-1]
            
            if current_price > price_1y_ago:
                pct_change = ((current_price - price_1y_ago) / price_1y_ago) * 100
                higher_now.append((ticker, price_1y_ago, current_price, pct_change))
                print(f"{ticker}: ${price_1y_ago:.2f} -> ${current_price:.2f} (+{pct_change:.2f}%)")
        except Exception as e:
            print(f"Error checking {ticker}: {e}")
            continue
    
    return higher_now

def compare_holdings():
    files = get_sorted_hdge_csvs()
    if len(files) < 2:
        msg = "Not enough HDGE CSV files to compare."
        print(msg)
        record_etf_change(None, None, [], [], note=msg)
        return
    
    latest = files[-1]
    previous = files[-2]
    print(f"Comparing HDGE: {os.path.basename(previous)} -> {os.path.basename(latest)}")
    
    df_latest = pd.read_csv(latest, skip_blank_lines=True, dtype=str)
    df_prev = pd.read_csv(previous, skip_blank_lines=True, dtype=str)
    
    df_latest = clean_holdings_df(df_latest)
    df_prev = clean_holdings_df(df_prev)
    
    latest_tickers = set(df_latest['Ticker'])
    prev_tickers = set(df_prev['Ticker'])
    
    added = sorted(latest_tickers - prev_tickers)
    removed = sorted(prev_tickers - latest_tickers)
    
    if added:
        print("\nAdditions:")
        for t in added:
            print(f"  {t}")
    else:
        print("\nNo additions.")
    
    if removed:
        print("\nRemovals:")
        for t in removed:
            print(f"  {t}")
    else:
        print("\nNo removals.")
    
    # Check price increases for all current holdings
    print("\nChecking price changes for current holdings (1 year lookback)...")
    higher_now = check_price_increases(sorted(latest_tickers))
    
    record_etf_change(previous, latest, added, removed, higher_now)

def record_etf_change(old_file, new_file, added, removed, higher_now=None, note=None):
    """
    Append a timestamped entry to output/ETF-Changes.txt describing HDGE additions/removals and price increases.
    """
    path = os.path.join(OUTPUT_DIR, "ETF-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- {ts} | HDGE ETF | Compared: {os.path.basename(old_file) if old_file else 'N/A'} -> {os.path.basename(new_file) if new_file else 'N/A'} ---\n")
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
        
        if higher_now:
            f.write("\nSecurities with Higher Prices (vs 1 year ago):\n")
            for ticker, old_price, new_price, pct in higher_now:
                f.write(f"  {ticker}: ${old_price:.2f} -> ${new_price:.2f} (+{pct:.2f}%)\n")
        else:
            f.write("\nSecurities with Higher Prices: None\n")
        
        f.write("\n")
    print(f"Changes appended to {path}")

if __name__ == "__main__":
    try:
        fetch_hdge_holdings()
    except Exception as e:
        print(f"Error downloading HDGE holdings: {e}")
    else:
        compare_holdings()

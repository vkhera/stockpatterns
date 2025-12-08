import os
import glob
import pandas as pd
from datetime import datetime

# ensure output dir exists and use it for BUZZ files
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_sorted_buzz_csvs():
    # look for BUZZ files in output folder
    files = glob.glob(os.path.join(OUTPUT_DIR, "BUZZ_asof_*.csv"))
    # Extract date from filename and sort descending
    files = sorted(files, key=lambda x: x[-12:-4], reverse=True)
    return files

def clean_holdings_df(df):
    # Find the header row (row with 'Ticker' and 'Holding Name')
    header_row = None
    for i, row in df.iterrows():
        cols = [str(c).strip().lower() for c in row]
        if "ticker" in cols and ("holding name" in cols or "name" in cols):
            header_row = i
            break
    if header_row is not None:
        df = pd.read_csv(df.attrs['filepath_or_buffer'], skiprows=header_row+1)
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
        # still write a record with timestamp
        record_change([], [], note=msg)
        return
    latest, previous = files[0], files[1]
    print(f"Comparing latest: {latest} with previous: {previous}")
    # Read and clean both files
    df_latest = pd.read_csv(latest, skip_blank_lines=True, dtype=str)
    df_prev = pd.read_csv(previous, skip_blank_lines=True, dtype=str)
    # Try to find header row and clean
    for df in [df_latest, df_prev]:
        df.attrs['filepath_or_buffer'] = latest if df is df_latest else previous
    df_latest = clean_holdings_df(df_latest)
    df_prev = clean_holdings_df(df_prev)
    # Compare by Ticker
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
    # write record to BUZZ-Changes.txt in output folder
    record_change(added, removed)

def record_change(added, removed, note=None):
    """
    Append a timestamped entry to output/BUZZ-Changes.txt describing additions/removals.
    """
    path = os.path.join(OUTPUT_DIR, "BUZZ-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- {ts} ---\n")
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

def get_all_buzz_files_chronological():
    """
    Return list of BUZZ_asof_YYYYMMDD.csv files in OUTPUT_DIR sorted ascending by date.
    """
    files = glob.glob(os.path.join(OUTPUT_DIR, "BUZZ_asof_*.csv"))
    def extract_date(f):
        try:
            return datetime.strptime(os.path.basename(f)[-12:-4], "%Y%m%d")
        except Exception:
            return datetime.fromtimestamp(os.path.getmtime(f))
    files_sorted = sorted(files, key=extract_date)
    return files_sorted

def compare_pair(file_old, file_new):
    """Compare two files and return (added, removed)."""
    df_old = pd.read_csv(file_old, skip_blank_lines=True, dtype=str)
    df_new = pd.read_csv(file_new, skip_blank_lines=True, dtype=str)
    df_old.attrs['filepath_or_buffer'] = file_old
    df_new.attrs['filepath_or_buffer'] = file_new
    df_old = clean_holdings_df(df_old)
    df_new = clean_holdings_df(df_new)
    old_tickers = set(df_old['Ticker'])
    new_tickers = set(df_new['Ticker'])
    added = sorted(new_tickers - old_tickers)
    removed = sorted(old_tickers - new_tickers)
    return added, removed

def record_change_for_pair(file_old, file_new, added, removed, note=None):
    """
    Append a timestamped entry for this pair to output/BUZZ-Changes.txt
    """
    path = os.path.join(OUTPUT_DIR, "BUZZ-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"--- {ts} | Compared: {os.path.basename(file_old)} -> {os.path.basename(file_new)} ---\n"
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

def compare_all_incremental():
    """
    Compare every adjacent pair of BUZZ files (chronological) and print/write differences.
    """
    files = get_all_buzz_files_chronological()
    if len(files) < 2:
        msg = "Not enough BUZZ CSV files (need >=2) to perform incremental comparisons."
        print(msg)
        record_change_for_pair("", "", [], [], note=msg)
        return
    for i in range(1, len(files)):
        prev_f = files[i-1]
        curr_f = files[i]
        print(f"\nComparing {os.path.basename(prev_f)} -> {os.path.basename(curr_f)}")
        try:
            added, removed = compare_pair(prev_f, curr_f)
        except Exception as e:
            err = f"Error comparing files {prev_f} and {curr_f}: {e}"
            print(err)
            record_change_for_pair(prev_f, curr_f, [], [], note=err)
            continue
        if added:
            print("  Additions:")
            for t in added:
                print(f"    {t}")
        else:
            print("  Additions: None")
        if removed:
            print("  Removals:")
            for t in removed:
                print(f"    {t}")
        else:
            print("  Removals: None")
        record_change_for_pair(prev_f, curr_f, added, removed)

if __name__ == "__main__":
    compare_all_incremental()

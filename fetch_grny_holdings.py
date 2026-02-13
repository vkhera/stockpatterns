from playwright.sync_api import sync_playwright
import os
import time
import shutil
import glob
from datetime import datetime
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

GRNY_URL = "https://grannyshots.com/fundstrat-granny-shots-us-large-cap-etf/grny-holdings/"
DOWNLOADS_DIR = os.path.expanduser("~/Downloads")

def fetch_grny_holdings():
    """Download GRNY ETF holdings CSV using Playwright"""
    
    with sync_playwright() as p:
        logger.info("Launching browser...")
        browser = p.chromium.launch(
            headless=False,  # Keep visible to see what's happening
            args=['--start-maximized']
        )
        
        context = browser.new_context(
            viewport=None,
            accept_downloads=True,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = context.new_page()
        
        try:
            # Navigate to GRNY holdings page
            logger.info(f"Navigating to {GRNY_URL}...")
            page.goto(GRNY_URL, wait_until="networkidle", timeout=60000)
            logger.info("Page loaded successfully")
            
            # Wait for page to fully load
            page.wait_for_timeout(3000)
            
            # Look for "Export to CSV" link
            logger.info("Looking for 'Export to CSV' link...")
            csv_selectors = [
                "a:has-text('Export to CSV')",
                "text=Export to CSV",
                "a[href*='csv' i]:has-text('Export')",
                "[class*='export']:has-text('CSV')",
                "button:has-text('Export to CSV')"
            ]
            
            # Set up download handler
            with page.expect_download(timeout=30000) as download_info:
                csv_clicked = False
                for selector in csv_selectors:
                    try:
                        logger.info(f"Trying selector: {selector}")
                        page.click(selector, timeout=5000)
                        csv_clicked = True
                        logger.info("'Export to CSV' link clicked successfully")
                        break
                    except Exception as e:
                        logger.debug(f"Selector failed: {e}")
                        continue
                
                if not csv_clicked:
                    logger.error("Could not find 'Export to CSV' link")
                    # List available links for debugging
                    logger.info("Available links on page:")
                    links = page.query_selector_all("a")
                    for link in links[:20]:
                        logger.info(f"  - {link.inner_text()}")
                    raise Exception("Export to CSV link not found")
            
            # Get the download
            download = download_info.value
            logger.info(f"Download started: {download.suggested_filename}")
            
            # Save to a temp location first
            temp_path = os.path.join(OUTPUT_DIR, download.suggested_filename)
            download.save_as(temp_path)
            logger.info(f"File saved to: {temp_path}")
            
            # Rename with date suffix for consistency
            today = datetime.now().strftime("%Y%m%d")
            new_filename = f"GRNY_holdings_{today}.csv"
            final_path = os.path.join(OUTPUT_DIR, new_filename)
            
            # If file exists, remove it first
            if os.path.exists(final_path):
                os.remove(final_path)
            
            shutil.move(temp_path, final_path)
            logger.info(f"File renamed to: {final_path}")
            
            # Verify file exists
            if os.path.exists(final_path):
                file_size = os.path.getsize(final_path)
                logger.info(f"Success! File size: {file_size} bytes")
                return final_path
            else:
                logger.error("File was not created successfully")
                return None
            
        except Exception as e:
            logger.error(f"Error during download process: {e}", exc_info=True)
            
            # Take screenshot on error
            try:
                screenshot_path = os.path.join(OUTPUT_DIR, f'grny_error_screenshot_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png')
                page.screenshot(path=screenshot_path, full_page=True)
                logger.info(f"Screenshot saved to: {screenshot_path}")
            except:
                pass
            
            return None
        
        finally:
            logger.info("Closing browser...")
            page.wait_for_timeout(2000)
            browser.close()

def move_from_downloads_to_output():
    """
    Alternative method: Monitor Downloads folder and move GRNY CSV file.
    Use this if direct download handling doesn't work.
    """
    try:
        logger.info(f"Looking for GRNY CSV in Downloads folder: {DOWNLOADS_DIR}")
        
        # Look for files matching pattern
        pattern = os.path.join(DOWNLOADS_DIR, "GRNY Holdings*.csv")
        files = glob.glob(pattern)
        
        if not files:
            logger.warning("No GRNY Holdings CSV found in Downloads folder")
            return None
        
        # Get the most recent file
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f"Found file: {latest_file}")
        
        # Create dated filename
        today = datetime.now().strftime("%Y%m%d")
        new_filename = f"GRNY_holdings_{today}.csv"
        dest_path = os.path.join(OUTPUT_DIR, new_filename)
        
        # Move file
        shutil.move(latest_file, dest_path)
        logger.info(f"File moved to: {dest_path}")
        
        return dest_path
        
    except Exception as e:
        logger.error(f"Error moving file from Downloads: {e}")
        return None

def get_sorted_grny_csvs():
    """Get list of GRNY holdings CSV files sorted by date (newest first)"""
    files = glob.glob(os.path.join(OUTPUT_DIR, "GRNY_holdings_*.csv"))
    files = sorted(files, key=lambda x: x.split('_')[-1].replace('.csv', ''), reverse=True)
    return files

def clean_holdings_df(df):
    """Clean GRNY holdings DataFrame to extract ticker symbols"""
    # Remove any rows where ticker is null, empty, or contains cash/other markers
    if 'Ticker' in df.columns:
        ticker_col = 'Ticker'
    elif 'Symbol' in df.columns:
        ticker_col = 'Symbol'
    elif 'TICKER' in df.columns:
        ticker_col = 'TICKER'
    elif 'SYMBOL' in df.columns:
        ticker_col = 'SYMBOL'
    else:
        # Try to find first column that looks like tickers
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].str.len().mean() < 10:
                ticker_col = col
                break
        else:
            logger.warning("Could not find ticker column")
            return df
    
    df = df[df[ticker_col].notnull() & df[ticker_col].astype(str).str.strip().ne('')]
    df = df[~df[ticker_col].str.contains('CASH', na=False, case=False)]
    df = df[~df[ticker_col].str.contains('--', na=False)]
    df[ticker_col] = df[ticker_col].astype(str).str.strip().str.upper()
    
    # Rename to standard 'Ticker' column
    if ticker_col != 'Ticker':
        df = df.rename(columns={ticker_col: 'Ticker'})
    
    return df

def compare_holdings():
    """Compare latest GRNY holdings with previous version and report changes"""
    files = get_sorted_grny_csvs()
    
    if len(files) < 2:
        msg = "Not enough GRNY CSV files to compare."
        logger.info(msg)
        record_etf_change(None, None, [], [], note=msg)
        return
    
    latest, previous = files[0], files[1]
    logger.info(f"Comparing latest: {os.path.basename(latest)} with previous: {os.path.basename(previous)}")
    
    try:
        df_latest = pd.read_csv(latest, dtype=str, on_bad_lines='skip')
        df_prev = pd.read_csv(previous, dtype=str, on_bad_lines='skip')
        
        df_latest = clean_holdings_df(df_latest)
        df_prev = clean_holdings_df(df_prev)
        
        latest_tickers = set(df_latest['Ticker'])
        prev_tickers = set(df_prev['Ticker'])
        
        added = sorted(latest_tickers - prev_tickers)
        removed = sorted(prev_tickers - latest_tickers)
        
        if added:
            logger.info("\nAdditions (new holdings in latest):")
            for t in added:
                logger.info(f"  {t}")
        else:
            logger.info("\nNo additions.")
        
        if removed:
            logger.info("\nRemovals (holdings no longer present):")
            for t in removed:
                logger.info(f"  {t}")
        else:
            logger.info("\nNo removals.")
        
        # Record changes to ETF-Changes.txt
        record_etf_change(previous, latest, added, removed)
        
    except Exception as e:
        logger.error(f"Error comparing holdings: {e}", exc_info=True)
        record_etf_change(previous, latest, [], [], note=f"Error during comparison: {e}")

def record_etf_change(old_file, new_file, added, removed, note=None):
    """
    Append a timestamped entry to output/ETF-Changes.txt describing GRNY ETF additions/removals.
    """
    path = os.path.join(OUTPUT_DIR, "ETF-Changes.txt")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"--- {ts} | GRNY ETF | Compared: {os.path.basename(old_file) if old_file else 'N/A'} -> {os.path.basename(new_file) if new_file else 'N/A'} ---\n")
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
    
    logger.info(f"Changes appended to {path}")

if __name__ == "__main__":
    logger.info("=== GRNY Holdings Download ===")
    logger.info("This script will download GRNY ETF holdings CSV")
    logger.info("=" * 50)
    
    try:
        result = fetch_grny_holdings()
        
        if result:
            logger.info(f"\n=== Download Completed Successfully ===")
            logger.info(f"File saved to: {result}")
            
            # Compare with previous version
            logger.info("\n=== Comparing Holdings ===")
            compare_holdings()
        else:
            logger.warning("\n=== Download Failed ===")
            logger.info("Trying alternative method: checking Downloads folder...")
            result = move_from_downloads_to_output()
            
            if result:
                logger.info(f"File found and moved to: {result}")
                
                # Compare with previous version
                logger.info("\n=== Comparing Holdings ===")
                compare_holdings()
            else:
                logger.error("Could not find or move GRNY CSV file")
                
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)

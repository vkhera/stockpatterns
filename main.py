from robinhood import RobinhoodPortfolio
from UpdateMySqlDB import insert_robinhood_holdings, main as insert_brokerage_holdings, store_llm_responses_to_mysql
import subprocess
import sys
import os
from datetime import datetime

# Setup logging to both console and file
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)
log_path = os.path.join(OUTPUT_DIR, 'main.log')

class TeeOutput:
    def __init__(self, *files):
        self.files = files
    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()

log_file = open(log_path, 'a', encoding='utf-8')
log_file.write(f"\n{'='*60}\nRun started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*60}\n")
sys.stdout = TeeOutput(sys.stdout, log_file)
sys.stderr = TeeOutput(sys.stderr, log_file)

robin = RobinhoodPortfolio('config.json')
#robin.download_holdings()
#robin.download_holdings_all_positions()
#robin.download_portfolio_profile()
robin.download_open_stock_positions()
print("Download activity CSV.")
robin.download_activity_csv()
robin.calculate_portfolio_risk()
print("Portfolio risk calculated.")
robin.fetch_and_analyze_news()
print("News fetched and analyzed.")
robin.analyze_trends()
print("Trends analyzed.")

def is_connected_to_ssid(target_ssids=("chup", "network")):
    """
    Returns True if connected to any of the specified WiFi SSIDs, False otherwise.
    Works on Windows using netsh.
    """
    try:
        output = subprocess.check_output(["netsh", "wlan", "show", "interfaces"], encoding="utf-8")
        for line in output.splitlines():
            if "SSID" in line and "BSSID" not in line:
                ssid = line.split(":", 1)[1].strip()
                return ssid in target_ssids
    except Exception:
        pass
    return False

# Insert Robinhood holdings into MySQL
def insert_robinhood_to_mysql():
    if is_connected_to_ssid():
        insert_robinhood_holdings()
    else:
        print("Not connected to WiFi SSID 'chup' or 'network'. Skipping Robinhood holdings DB insert.")

# Insert brokerage (holdings_cleaned.csv) into MySQL
def insert_brokerage_to_mysql():
    if is_connected_to_ssid():
        insert_brokerage_holdings()
    else:
        print("Not connected to WiFi SSID 'chup' or 'network'. Skipping brokerage holdings DB insert.")

# Accuracy calculation
from Accuracy import calculate_model_accuracy_and_timing

def store_llm_responses():
    if is_connected_to_ssid():
        store_llm_responses_to_mysql()
    else:
        print("Not connected to WiFi SSID 'chup' or 'network'. Skipping LLM responses DB insert.")

# Call accuracy calculation at the end
calculate_model_accuracy_and_timing()
print("Accuracy stats written to output/Accuracy.csv.")

# Example usage:
#insert_robinhood_to_mysql()
#insert_brokerage_to_mysql()
store_llm_responses()
print("LLM responses stored in MySQL.")

# Analyze leveraged ETFs at the end
from leveraged_etf_analysis import analyze_leveraged_etfs
print("\nAnalyzing leveraged ETFs...")
analyze_leveraged_etfs()
print("Leveraged ETF analysis complete.")

# At the end, close log file
log_file.close()

import robin_stocks as r
import pandas as pd
import yfinance as yf
import json
from cryptography.fernet import Fernet
import numpy as np
import os
import glob

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_latest_holdings_csv():
    """
    Returns the path to the latest holdings_report_open_stock_positions_yyyymmdd.csv file,
    or 'holdings_report.csv' if none found. Prints which file is being used.
    """
    pattern = os.path.join(OUTPUT_DIR, "holdings_report_open_stock_positions_*.csv")
    files = glob.glob(pattern)
    if files:
        latest_file = max(files, key=os.path.getmtime)
        print(f"Using latest open stock positions file for analysis: {latest_file}")
        return latest_file
    fallback = os.path.join(OUTPUT_DIR, "holdings_report.csv")
    print(f"No dated open stock positions file found. Using fallback: {fallback}")
    return fallback

def get_latest_news_file():
    """
    Returns the path to the latest robin-news_*.txt file, or None if not found.
    """
    pattern = os.path.join(OUTPUT_DIR, "robin-news_*.txt")
    files = glob.glob(pattern)
    if files:
        latest_file = max(files, key=os.path.getmtime)
        print(f"Found previous news file: {latest_file}")
        return latest_file
    print("No previous news file found. All items will be analyzed.")
    return None

def parse_previous_news(news_file_path):
    """
    Parse a previous robin-news_*.txt file and extract headlines and URLs.
    Returns a set of (headline, url) tuples for quick lookup.
    """
    if not news_file_path or not os.path.exists(news_file_path):
        return set()
    
    previous_items = set()
    try:
        with open(news_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            # Split by symbol sections
            sections = content.split('='*60)
            for section in sections:
                lines = section.strip().split('\n')
                headline = None
                url = None
                for line in lines:
                    if line.startswith('Headline: '):
                        headline = line.replace('Headline: ', '').strip()
                    elif line.startswith('URL: '):
                        url = line.replace('URL: ', '').strip()
                    # When we have both, add to set
                    if headline and url:
                        previous_items.add((headline, url))
                        headline = None
                        url = None
        print(f"Loaded {len(previous_items)} previous news items for comparison.")
    except Exception as e:
        print(f"Error parsing previous news file: {e}")
    return previous_items

class RobinhoodPortfolio:
    def __init__(self, config_path='config.json'):
        with open(config_path, 'r') as f:
            config = json.load(f)
        self.username = config['username']
        key = config['key'].encode()
        fernet = Fernet(key)
        self.password = fernet.decrypt(config['password'].encode()).decode()

    def login(self):
        login = r.robinhood.authentication.login(self.username, self.password)
        if not login.get('access_token'):
            raise Exception('Login failed. Please check your credentials.')
        return login

    def download_holdings(self, output_csv=None):
        self.login()
        holdings = r.robinhood.account.build_holdings()
        if not holdings:
            print('No holdings found.')
            return None
        data = []
        if output_csv is None:
            output_csv = os.path.join(OUTPUT_DIR, 'holdings_report.csv')
        for symbol, info in holdings.items():
            print(f"{symbol}: {info['quantity']} shares @ ${info['price']} each, Equity: ${info['equity']}")
            # Fetch beta using yfinance
            try:
                ticker = yf.Ticker(symbol)
                beta = ticker.info.get('beta', 'N/A')
            except Exception:
                beta = 'N/A'
            # Trend analysis (30-day slope)
            try:
                hist = yf.Ticker(symbol).history(period='30d')['Close']
                if len(hist) >= 15:
                    from scipy.stats import linregress
                    x = range(len(hist))
                    y = hist.values
                    slope, _, _, _, _ = linregress(x, y)
                    threshold = 0.001 * y[0]  # 0.1% of starting price per day
                    if slope > threshold:
                        trend = 'uptrend'
                    elif slope < -threshold:
                        trend = 'downtrend'
                    else:
                        trend = 'sideways'
                else:
                    trend = 'not enough data'
            except Exception:
                trend = 'error'
            data.append({
                'Symbol': symbol,
                'Quantity': info['quantity'],
                'Price': info['price'],
                'Equity': info['equity'],
                'Percent Change': info['percent_change'],
                'Type': info['type'],
                'Beta': beta,
                'Trend': trend
            })
        df = pd.DataFrame(data)
        df.to_csv(output_csv, index=False)
        print(f"Holdings exported to {output_csv} with Beta and Trend columns.")
        return df

    def download_holdings_all_positions(self, output_csv=None):
        """
        Download holdings using get_all_positions from robin_stocks and export to CSV (with Beta and Trend columns).
        """
        self.login()
        positions = r.robinhood.account.get_all_positions()
        if not positions:
            print('No positions found.')
            return None
        data = []
        if output_csv is None:
            output_csv = os.path.join(OUTPUT_DIR, 'holdings_report_all_positions.csv')
        for pos in positions:
            instrument_url = pos.get('instrument')
            symbol = None
            try:
                if instrument_url:
                    instrument = r.robinhood.stocks.get_instrument_by_url(instrument_url)
                    symbol = instrument.get('symbol')
            except Exception:
                symbol = None
            if not symbol:
                continue
            quantity = pos.get('quantity', '0')
            average_buy_price = pos.get('average_buy_price', '0')
            equity = float(quantity) * float(average_buy_price)
            # Fetch beta using yfinance
            try:
                ticker = yf.Ticker(symbol)
                beta = ticker.info.get('beta', 'N/A')
            except Exception:
                beta = 'N/A'
            # Trend analysis (30-day slope)
            try:
                hist = yf.Ticker(symbol).history(period='30d')['Close']
                if len(hist) >= 15:
                    from scipy.stats import linregress
                    x = range(len(hist))
                    y = hist.values
                    slope, _, _, _, _ = linregress(x, y)
                    threshold = 0.001 * y[0]  # 0.1% of starting price per day
                    if slope > threshold:
                        trend = 'uptrend'
                    elif slope < -threshold:
                        trend = 'downtrend'
                    else:
                        trend = 'sideways'
                else:
                    trend = 'not enough data'
            except Exception:
                trend = 'error'
            data.append({
                'Symbol': symbol,
                'Quantity': quantity,
                'Price': average_buy_price,
                'Equity': equity,
                'Percent Change': pos.get('percent_change', 'N/A'),
                'Type': pos.get('type', 'N/A'),
                'Beta': beta,
                'Trend': trend
            })
        df = pd.DataFrame(data)
        df.to_csv(output_csv, index=False)
        print(f"Holdings (all positions) exported to {output_csv} with Beta and Trend columns.")
        return df

    def download_open_stock_positions(self, output_csv=None):
        """
        Download open stock positions for all accounts in config.json and export to CSV (with Beta and Trend columns).
        """
        self.login()
        # Read account numbers from config.json
        with open('config.json', 'r') as f:
            config = json.load(f)
        account_numbers = config.get('account_numbers', [])
        if isinstance(account_numbers, str):
            account_numbers = [account_numbers]
        if not account_numbers:
            print('No account numbers found in config.json (key: account_numbers).')
            return None
        all_positions = []
        for account in account_numbers:
            positions = r.robinhood.account.get_open_stock_positions(account)
            if not positions:
                print(f'No open stock positions found for account {account}.')
                continue
            all_positions.extend(positions)
        if not all_positions:
            print('No open stock positions found for any account.')
            return None
        data = []
        from datetime import datetime
        today_str = datetime.now().strftime("%Y%m%d")
        if output_csv is None:
            output_csv = os.path.join(OUTPUT_DIR, f'holdings_report_open_stock_positions_{today_str}.csv')
        for pos in all_positions:
            instrument_url = pos.get('instrument')
            symbol = None
            try:
                if instrument_url:
                    instrument = r.robinhood.stocks.get_instrument_by_url(instrument_url)
                    symbol = instrument.get('symbol')
            except Exception:
                symbol = None
            if not symbol:
                continue
            quantity = pos.get('quantity', '0')
            average_buy_price = pos.get('average_buy_price', '0')
            equity = float(quantity) * float(average_buy_price)
            # Fetch beta using yfinance
            try:
                ticker = yf.Ticker(symbol)
                beta = ticker.info.get('beta', 'N/A')
            except Exception:
                beta = 'N/A'
            # Trend analysis (30-day slope)
            try:
                hist = yf.Ticker(symbol).history(period='30d')['Close']
                if len(hist) >= 15:
                    from scipy.stats import linregress
                    x = range(len(hist))
                    y = hist.values
                    slope, _, _, _, _ = linregress(x, y)
                    threshold = 0.001 * y[0]  # 0.1% of starting price per day
                    if slope > threshold:
                        trend = 'uptrend'
                    elif slope < -threshold:
                        trend = 'downtrend'
                    else:
                        trend = 'sideways'
                else:
                    trend = 'not enough data'
            except Exception:
                trend = 'error'
            data.append({
                'Symbol': symbol,
                'Quantity': quantity,
                'Price': average_buy_price,
                'Equity': equity,
                'Percent Change': pos.get('percent_change', 'N/A'),
                'Type': pos.get('type', 'N/A'),
                'Beta': beta,
                'Trend': trend
            })
        df = pd.DataFrame(data)
        df.to_csv(output_csv, index=False)
        print(f"Open stock positions exported to {output_csv} with Beta and Trend columns (all accounts combined).")
        return df

    def calculate_portfolio_risk(self, holdings_csv=None):
        if holdings_csv is None:
            holdings_csv = get_latest_holdings_csv()
        df = pd.read_csv(holdings_csv)
        for col in ['Equity', 'Beta']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        total_equity = df['Equity'].sum()
        df['Weight'] = df['Equity'] / total_equity
        portfolio_beta = (df['Weight'] * df['Beta']).sum()
        with open(os.path.join(OUTPUT_DIR, 'portfolio_beta.txt'), 'w') as f:
            f.write('Portfolio Beta Calculation Details\n')
            f.write('---------------------------------\n')
            f.write(df[['Symbol', 'Equity', 'Weight', 'Beta']].to_string(index=False))
            f.write(f"\n\nTotal Equity: {total_equity:.2f}\n")
            f.write("\nWeighted Beta Calculation (Weight * Beta for each holding):\n")
            for _, row in df.iterrows():
                f.write(f"{row['Symbol']}: {row['Weight']:.4f} * {row['Beta']:.4f} = {row['Weight']*row['Beta']:.4f}\n")
            f.write(f"\nPortfolio Beta: {portfolio_beta:.4f}\n")
        # Standard deviation based risk
        returns = []
        symbols = df['Symbol'].tolist()
        valid_symbols = []
        for symbol in symbols:
            try:
                data = yf.Ticker(symbol).history(period='1y')['Close']
                ret = data.pct_change().dropna()
                if not ret.empty:
                    returns.append(ret)
                    valid_symbols.append(symbol)
            except Exception:
                returns.append(pd.Series(dtype=float))
        if not returns or not valid_symbols:
            print("No valid returns data for risk calculation.")
            return portfolio_beta, None
        returns_df = pd.concat(returns, axis=1)
        returns_df.columns = valid_symbols
        # Only use weights for valid symbols
        weights = df.set_index('Symbol').loc[valid_symbols, 'Weight'].values
        cov_matrix = returns_df.cov()
        if cov_matrix.shape[0] != len(weights):
            print("Covariance matrix and weights shape mismatch. Skipping std calculation.")
            portfolio_std = None
        else:
            portfolio_var = np.dot(weights.T, np.dot(cov_matrix, weights))
            portfolio_std = np.sqrt(portfolio_var)
            with open(os.path.join(OUTPUT_DIR, 'portfolio_std.txt'), 'w') as f:
                f.write('Portfolio Standard Deviation Calculation Details\n')
                f.write('----------------------------------------------\n')
                f.write('Weights:\n')
                for symbol, weight in zip(valid_symbols, weights):
                    f.write(f"{symbol}: {weight:.4f}\n")
                f.write('\nCovariance Matrix:\n')
                f.write(cov_matrix.to_string())
                f.write(f"\n\nPortfolio Variance: {portfolio_var:.8f}\n")
                f.write(f"Portfolio Standard Deviation (annualized): {portfolio_std:.4%}\n")
        print("Portfolio risk metrics exported to portfolio_beta.txt and portfolio_std.txt.")
        return portfolio_beta, portfolio_std

    def analyze_sentiment_with_ollama(self, symbol, headline, summary, ollama_url, logf, models=None, csv_log_path=None):
        """
        Call Ollama API sequentially for multiple model ids for a news item. Logs request, response, and timing to a CSV file.
        Returns the majority sentiment out of the 3 calls. If all 3 are different, returns the last call's response.
        """
        import requests
        import time
        import csv as pycsv
        from collections import Counter
        if models is None:
            models = ["gemma3:1b", "mistral:7b", "llama3.2:latest"]
          #  models = ["gemma3:1b", "mistral:7b", "llama3.2:latest","gemma3n:latest","gpt-oss:20b"]
        prompt = f"Analyze the following news for sentiment (positive, negative, neutral) for the stock {symbol}. Respond with one word: positive, negative, or neutral.\nNews: {headline} {summary}"
        results = {}
        responses = []
        csv_rows = []
        for model_id in models:
            start_time = time.time()
            req_payload = {"model": model_id, "prompt": prompt, "stream": False}
            logf.write(f"\n{'='*40}\nSYMBOL: {symbol}\nMODEL: {model_id}\nPROMPT:\n{prompt}\n")
            req_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
            try:
                response = requests.post(ollama_url, json=req_payload)
                resp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                logf.write(f"RESPONSE RAW:\n{response.text}\n")
                if response.ok:
                    result = response.json()
                    analysis = result.get('response', '').strip().lower()
                else:
                    analysis = 'unknown'
            except Exception as e:
                analysis = f'error: {e}'
                resp_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            exec_time = time.time() - start_time
            results[model_id] = analysis
            responses.append(analysis)
            # majority_response will be determined after all responses are collected
            csv_rows.append([
                symbol,
                model_id,
                req_time,
                prompt,
                resp_time,
                analysis,
                '',  # placeholder for majority, to be filled after loop
                f"{exec_time:.3f}"
            ])
        # Determine majority response
        filtered = [resp for resp in responses if resp in ("positive", "negative", "neutral")]
        if filtered:
            count = Counter(filtered)
            most_common = count.most_common()
            if len(most_common) == 1:
                majority_response = most_common[0][0]
            elif len(most_common) > 1 and most_common[0][1] > 1:
                majority_response = most_common[0][0]
            else:
                majority_response = responses[-1]
        else:
            majority_response = responses[-1] if responses else 'unknown'

        # Fill in majority response for all rows
        for row in csv_rows:
            row[6] = majority_response

        # Always write to llm_response_record.csv in output folder
        output_dir = os.path.join(os.path.dirname(__file__), 'output')
        llm_csv_path = os.path.join(output_dir, 'llm_response_record.csv')
        write_header = not os.path.exists(llm_csv_path)
        with open(llm_csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = pycsv.writer(csvfile)
            if write_header:
                writer.writerow(["Symbol", "Model", "Request Time", "Prompt", "Response Time", "Analysis", "Majority", "Exec Time (s)"])
            writer.writerows(csv_rows)
        # Write to CSV log if path provided (legacy/optional)
        if csv_log_path:
            write_header2 = not os.path.exists(csv_log_path)
            with open(csv_log_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = pycsv.writer(csvfile)
                if write_header2:
                    writer.writerow(["Symbol", "Model", "Request Time", "Prompt", "Response Time", "Analysis", "Majority", "Exec Time (s)"])
                writer.writerows(csv_rows)
        return majority_response

    def fetch_and_analyze_news(self, holdings_csv=None, news_output=None, ollama_url='http://localhost:11434/api/generate', ollama_log=None):
        import requests
        import json as pyjson
        from datetime import datetime, timedelta, timezone
        if holdings_csv is None:
            holdings_csv = get_latest_holdings_csv()
        df = pd.read_csv(holdings_csv)
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        if news_output is None:
            news_output = os.path.join(OUTPUT_DIR, f'robin-news_{timestamp}.txt')
        if ollama_log is None:
            ollama_log = os.path.join(OUTPUT_DIR, f'ollama_news_log_{timestamp}.txt')
        
        # Load previous news items for comparison
        previous_news_file = get_latest_news_file()
        previous_items = parse_previous_news(previous_news_file)
        
        new_items_count = 0
        reused_items_count = 0
        
        with open(news_output, 'w', encoding='utf-8') as f, open(ollama_log, 'w', encoding='utf-8') as logf:
            f.write(f"News Analysis Report - {timestamp}\n")
            f.write(f"Previous news file: {previous_news_file if previous_news_file else 'None'}\n")
            f.write(f"Previous items loaded: {len(previous_items)}\n\n")
            
            for symbol in df['Symbol']:
                f.write(f"\n{'='*60}\n{symbol} News\n{'='*60}\n")
                wrote_any = False
                try:
                    news_items = r.robinhood.stocks.get_news(symbol)
                except Exception as e:
                    f.write(f"Error fetching news for {symbol}: {e}\n")
                    continue
                if not news_items:
                    f.write("No news found.\n")
                    continue
                for news in news_items:
                    # Filter by published date (last 4 days)
                    published_at = news.get('published_at')
                    if not published_at:
                        continue
                    try:
                        pub_dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
                    except Exception:
                        try:
                            pub_dt = datetime.strptime(published_at[:10], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        except Exception:
                            continue
                    if (now - pub_dt).days > 4:
                        continue
                    headline = news.get('title', '')
                    summary = news.get('summary', '')
                    url = news.get('url', '')
                    
                    # Check if this is a new item
                    item_key = (headline, url)
                    is_new = item_key not in previous_items
                    
                    # Only analyze sentiment for NEW items
                    if is_new:
                        analysis = self.analyze_sentiment_with_ollama(symbol, headline, summary, ollama_url, logf)
                        new_items_count += 1
                    else:
                        analysis = 'not analyzed (previously seen)'
                        reused_items_count += 1
                    
                    if analysis != 'neutral' or is_new:
                        status_marker = "[NEW]" if is_new else "[SEEN]"
                        f.write(f"{status_marker} Headline: {headline}\nSummary: {summary}\nURL: {url}\nSentiment: {analysis}\n\n")
                        wrote_any = True
                if not wrote_any:
                    f.write("No positive or negative news found.\n")
            
            # Write summary at the end
            f.write(f"\n\n{'='*60}\nSummary\n{'='*60}\n")
            f.write(f"New items analyzed: {new_items_count}\n")
            f.write(f"Previously seen items (not re-analyzed): {reused_items_count}\n")
        
        print(f"News and sentiment analysis written to {news_output}.")
        print(f"New items analyzed: {new_items_count}, Previously seen: {reused_items_count}")
        print(f"Ollama requests/responses logged in {ollama_log}.")

    def analyze_trends(self, holdings_csv=None, trend_output=None, lookback_days=30):
        import yfinance as yf
        from datetime import datetime
        from scipy.stats import linregress
        import pandas as pd
        if holdings_csv is None:
            holdings_csv = get_latest_holdings_csv()
        if trend_output is None:
            trend_output = os.path.join(OUTPUT_DIR, 'trend_analysis.txt')
        df = pd.read_csv(holdings_csv)
        today = datetime.now()
        with open(trend_output, 'w', encoding='utf-8') as f:
            for symbol in df['Symbol']:
                try:
                    data = yf.Ticker(symbol).history(period=f'{lookback_days}d')['Close']
                    if len(data) < lookback_days // 2:
                        f.write(f"{symbol}: Not enough data for trend analysis.\n")
                        continue
                    # Linear regression: x = days, y = price
                    x = range(len(data))
                    y = data.values
                    slope, _, _, _, _ = linregress(x, y)
                    # Heuristic: threshold for sideways
                    threshold = 0.001 * y[0]  # 0.1% of starting price per day
                    if slope > threshold:
                        trend = 'uptrend'
                    elif slope < -threshold:
                        trend = 'downtrend'
                    else:
                        trend = 'sideways'
                    f.write(f"{symbol}: {trend} (slope={slope:.4f})\n")
                except Exception as e:
                    f.write(f"{symbol}: Error analyzing trend: {e}\n")
        print(f"Trend analysis written to {trend_output}.")

    def download_portfolio_profile(self, output_json=None):
        """
        Download the portfolio profile using load_portfolio_profile from robin_stocks and export to JSON.
        """
        self.login()
        profile = r.robinhood.profiles.load_portfolio_profile()
        if not profile:
            print('No portfolio profile found.')
            return None
        if output_json is None:
            output_json = os.path.join(OUTPUT_DIR, 'portfolio_profile.json')
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(profile, f, indent=2)
        print(f"Portfolio profile exported to {output_json}.")
        return profile

    def download_activity_csv(self, output_dir=None):
        """
        Download account activity CSV using robin_stocks order history API.
        Saves file with date appended in format yyyymmdd.
        
        Args:
            output_dir (str): Directory to save CSV. Defaults to OUTPUT_DIR.
        
        Returns:
            str: Path to the downloaded CSV file
        """
        from datetime import datetime
        import pandas as pd
        import os
        
        self.login()
        
        if output_dir is None:
            output_dir = OUTPUT_DIR
        
        today_str = datetime.now().strftime("%Y%m%d")
        file_name = f"robinhood_activity_{today_str}.csv"
        csv_path = os.path.join(output_dir, file_name)
        
        try:
            print(f"Fetching order history from Robinhood...")
            
            # Get all stock orders (correct function name)
            orders = r.robinhood.orders.get_all_stock_orders()
            
            if not orders:
                print("No orders found in history.")
                return None
            
            print(f"Retrieved {len(orders)} orders")
            
            # Convert to DataFrame
            df = pd.DataFrame(orders)
            
            # Select and rename relevant columns
            columns_to_keep = [
                'created_at', 'updated_at', 'side', 'state', 
                'quantity', 'average_price', 'executions', 'instrument'
            ]
            
            # Keep only columns that exist
            available_cols = [col for col in columns_to_keep if col in df.columns]
            df_export = df[available_cols].copy()
            
            # Get instrument symbols
            print("Fetching instrument symbols...")
            symbols = []
            for instrument_url in df['instrument']:
                try:
                    instrument_data = r.robinhood.stocks.get_instrument_by_url(instrument_url)
                    symbols.append(instrument_data.get('symbol', 'UNKNOWN'))
                except:
                    symbols.append('UNKNOWN')
            
            df_export['symbol'] = symbols
            
            # Flatten executions data if available
            if 'executions' in df.columns:
                df_export['execution_price'] = df['executions'].apply(
                    lambda x: x[0].get('price', '') if isinstance(x, list) and len(x) > 0 else ''
                )
                df_export['execution_quantity'] = df['executions'].apply(
                    lambda x: x[0].get('quantity', '') if isinstance(x, list) and len(x) > 0 else ''
                )
            
            # Rename columns for clarity
            column_rename = {
                'created_at': 'Activity Date',
                'updated_at': 'Process Date',
                'side': 'Trans Code',
                'state': 'Status',
                'quantity': 'Quantity',
                'average_price': 'Price',
                'symbol': 'Instrument'
            }
            
            df_export = df_export.rename(columns=column_rename)
            
            # Save to CSV
            df_export.to_csv(csv_path, index=False)
            
            print(f"Activity CSV created successfully: {csv_path}")
            print(f"File size: {os.path.getsize(csv_path)} bytes")
            print(f"Total records: {len(df_export)}")
            
            return csv_path
            
        except Exception as e:
            print(f"Error downloading activity CSV: {e}")
            import traceback
            traceback.print_exc()
            return None

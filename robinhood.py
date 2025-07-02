import robin_stocks as r
import pandas as pd
import yfinance as yf
import json
from cryptography.fernet import Fernet
import numpy as np
import os

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
        if output_csv is None:
            output_csv = os.path.join(OUTPUT_DIR, 'holdings_report_open_stock_positions.csv')
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
            holdings_csv = os.path.join(OUTPUT_DIR, 'holdings_report.csv')
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
        for symbol in symbols:
            try:
                data = yf.Ticker(symbol).history(period='1y')['Close']
                ret = data.pct_change().dropna()
                returns.append(ret)
            except Exception:
                returns.append(pd.Series(dtype=float))
        returns_df = pd.concat(returns, axis=1)
        returns_df.columns = symbols
        cov_matrix = returns_df.cov()
        weights = df.set_index('Symbol').loc[symbols, 'Weight'].values
        portfolio_var = np.dot(weights.T, np.dot(cov_matrix, weights))
        portfolio_std = np.sqrt(portfolio_var)
        with open(os.path.join(OUTPUT_DIR, 'portfolio_std.txt'), 'w') as f:
            f.write('Portfolio Standard Deviation Calculation Details\n')
            f.write('----------------------------------------------\n')
            f.write('Weights:\n')
            for symbol, weight in zip(symbols, weights):
                f.write(f"{symbol}: {weight:.4f}\n")
            f.write('\nCovariance Matrix:\n')
            f.write(cov_matrix.to_string())
            f.write(f"\n\nPortfolio Variance: {portfolio_var:.8f}\n")
            f.write(f"Portfolio Standard Deviation (annualized): {portfolio_std:.4%}\n")
        print("Portfolio risk metrics exported to portfolio_beta.txt and portfolio_std.txt.")
        return portfolio_beta, portfolio_std

    def fetch_and_analyze_news(self, holdings_csv=None, news_output=None, ollama_url='http://localhost:11434/api/generate', ollama_log=None):
        import requests
        import json as pyjson
        from datetime import datetime, timedelta, timezone
        if holdings_csv is None:
            holdings_csv = os.path.join(OUTPUT_DIR, 'holdings_report.csv')
        df = pd.read_csv(holdings_csv)
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        if news_output is None:
            news_output = os.path.join(OUTPUT_DIR, f'robin-news_{timestamp}.txt')
        if ollama_log is None:
            ollama_log = os.path.join(OUTPUT_DIR, f'ollama_news_log_{timestamp}.txt')
        with open(news_output, 'w', encoding='utf-8') as f, open(ollama_log, 'w', encoding='utf-8') as logf:
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
                        # Try parsing ISO format, fallback to date only
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
                    # Analyze sentiment using Ollama
                    try:
                        prompt = f"Analyze the following news for sentiment (positive, negative, neutral) for the stock {symbol}. Respond with one word: positive, negative, or neutral.\nNews: {headline} {summary}"
                        logf.write(f"\n{'='*40}\nSYMBOL: {symbol}\nPROMPT:\n{prompt}\n")
                        response = requests.post(ollama_url, json={"model": "llama3.2:latest", "prompt": prompt, "stream": False})
                        logf.write(f"RESPONSE RAW:\n{response.text}\n")
                        if response.ok:
                            result = response.json()
                            analysis = result.get('response', '').strip().lower()
                        else:
                            analysis = 'unknown'
                    except Exception as e:
                        analysis = f'error: {e}'
                    if analysis != 'neutral':
                        f.write(f"Headline: {headline}\nSummary: {summary}\nURL: {url}\nSentiment: {analysis}\n\n")
                        wrote_any = True
                if not wrote_any:
                    f.write("No positive or negative news found.\n")
        print(f"News and sentiment analysis written to {news_output}. Ollama requests/responses logged in {ollama_log}.")

    def analyze_trends(self, holdings_csv=None, trend_output=None, lookback_days=30):
        import yfinance as yf
        from datetime import datetime
        from scipy.stats import linregress
        import pandas as pd
        if holdings_csv is None:
            holdings_csv = os.path.join(OUTPUT_DIR, 'holdings_report.csv')
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

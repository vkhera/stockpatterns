import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os

# Static list of popular US leveraged ETFs and their underlying tickers
LEVERAGED_ETFS = [
    # (ETF, Underlying)
    ("SPXL", "SPY"),   # Direxion Daily S&P 500 Bull 3X Shares
    ("TQQQ", "QQQ"),   # ProShares UltraPro QQQ
    ("QLD", "QQQ"),   # ProShares UltraPro QQQ
    ("UDOW", "DOW"),   # ProShares UltraPro QQQ
    ("SOXL", "SOXX"),  # Direxion Daily Semiconductor Bull 3X Shares
    ("LABU", "XBI"),   # Direxion Daily S&P Biotech Bull 3X Shares
    ("FAS", "XLF"),    # Direxion Daily Financial Bull 3X Shares
    ("TECL", "XLK"),   # Direxion Daily Technology Bull 3X Shares
    ("NUGT", "GDX"),   # Direxion Daily Gold Miners Bull 2X Shares
    ("DUST", "GDX"),   # Direxion Daily Gold Miners Bear 2X Shares
    ("UPRO", "SPY"),   # ProShares UltraPro S&P500
    ("UTSL", "XLU"),   # ProShares UltraPro Utilities
    ("MSFU", "MSFT"),   # ProShares UltraPro Microsoft
    ("AAPU", "AAPL"),    # Apple 2x Bull
    ("DUSL", "XLI"),    # ProShares UltraPro Industrials
    ("TNA", "IWM"),    # Russle 2000 3x Bull
    ("NAIL", "XHB"),    # ProShares UltraPro Silver
    ("PLTU", "PLTR"),    # Palantir 2x Bull
    ("FNGU", "MAGS"),    # Palantir 2x Bull
    ("AMZU", "AMZN"),    # Palantir 2x Bull
    ("DRN", "XLRE"),    # Palantir 2x Bull
    ("BULZ", "QQQ"),
    ("GRNY", "QQQ"),
    ("IVES", "QQQ"),
    ("ARKK", "QQQ"),    
    ("NRGU", "XLE"),    
    ("SPYU", "SPY"),
    ("BUZZ", "SPY")
    # Add more as needed
]

PERIODS = [
    ("1 Month", 30),
    ("6 Months", 182),
    ("YTD", None),  # Special handling
    ("1 Year", 365),
    ("3 Years", 365*3),
    ("5 Years", 365*5),
]

# add output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_return(ticker, start_date, end_date):
    data = yf.Ticker(ticker).history(start=start_date, end=end_date)
    if data.empty or len(data) < 2:
        return None
    start_price = data["Close"].iloc[0]
    end_price = data["Close"].iloc[-1]
    return (end_price - start_price) / start_price * 100

def get_ytd_return(ticker):
    now = datetime.now()
    start = datetime(now.year, 1, 1)
    return get_return(ticker, start, now)

def get_annualized_std(ticker, period_days=365):
    """
    Returns the annualized standard deviation (risk) of daily returns over the given period.
    """
    now = datetime.now()
    start = now - timedelta(days=period_days)
    data = yf.Ticker(ticker).history(start=start, end=now)
    if data.empty or len(data) < 2:
        return None
    returns = data["Close"].pct_change().dropna()
    if returns.empty:
        return None
    std = returns.std() * np.sqrt(252) * 100  # annualized, percent
    return std

def analyze_leveraged_etfs():
    now = datetime.now()
    results = []
    print(f"{'ETF':<6} {'Underlying':<10} {'Period':<8} {'ETF %':>8} {'Und %':>8} {'Diff %':>8}")
    print("-" * 50)
    for etf, underlying in LEVERAGED_ETFS:
        etf_perf = {}
        und_perf = {}
        for period, days in PERIODS:
            if period == "YTD":
                etf_ret = get_ytd_return(etf)
                und_ret = get_ytd_return(underlying)
            else:
                start = now - timedelta(days=days)
                etf_ret = get_return(etf, start, now)
                und_ret = get_return(underlying, start, now)
            etf_perf[period] = etf_ret
            und_perf[period] = und_ret
            if etf_ret is not None and und_ret is not None:
                diff = etf_ret - und_ret
                print(f"{etf:<6} {underlying:<10} {period:<8} {etf_ret:8.2f} {und_ret:8.2f} {diff:8.2f}")
            else:
                print(f"{etf:<6} {underlying:<10} {period:<8} {'N/A':>8} {'N/A':>8} {'N/A':>8}")
        results.append((etf, underlying, etf_perf, und_perf))
    # Find ETF with highest outperformance (sum of diffs)
    best_etf = None
    best_score = float('-inf')
    avg_diffs = []
    risk_map = {}
    for etf, underlying, etf_perf, und_perf in results:
        total_diff = 0
        count = 0
        for period in etf_perf:
            e = etf_perf[period]
            u = und_perf[period]
            if e is not None and u is not None:
                total_diff += e - u
                count += 1
        avg_diff = total_diff / count if count else float('-inf')
        avg_diffs.append((etf, underlying, avg_diff))
        if avg_diff > best_score:
            best_score = avg_diff
            best_etf = (etf, underlying, avg_diff)
        # Calculate risk (annualized std) for ETF and underlying
        etf_risk = get_annualized_std(etf)
        und_risk = get_annualized_std(underlying)
        risk_map[etf] = (etf_risk, und_risk)
    if best_etf:
        print("\nLeveraged ETF with highest average outperformance vs. underlying:")
        print(f"{best_etf[0]} (underlying: {best_etf[1]}) | Avg Outperformance: {best_etf[2]:.2f}%")

    # List all leveraged ETFs sorted by outperformance
    print("\nAll Leveraged ETFs sorted by average outperformance vs. underlying:")
    print(f"{'ETF':<6} {'Underlying':<10} {'Avg Outperf %':>14} {'ETF YTD %':>12} {'Und YTD %':>12} {'ETF Risk %':>12} {'Und Risk %':>12}")
    print("-" * 88)
    for etf, underlying, avg_diff in sorted(avg_diffs, key=lambda x: x[2], reverse=True):
        # Calculate YTD returns for summary table
        etf_ytd = get_ytd_return(etf)
        und_ytd = get_ytd_return(underlying)
        etf_ytd_str = f"{etf_ytd:10.2f}" if etf_ytd is not None else "    N/A   "
        und_ytd_str = f"{und_ytd:10.2f}" if und_ytd is not None else "    N/A   "
        etf_risk, und_risk = risk_map.get(etf, (None, None))
        etf_risk_str = f"{etf_risk:10.2f}" if etf_risk is not None else "    N/A   "
        und_risk_str = f"{und_risk:10.2f}" if und_risk is not None else "    N/A   "
        print(f"{etf:<6} {underlying:<10} {avg_diff:14.2f} {etf_ytd_str:>12} {und_ytd_str:>12} {etf_risk_str:>12} {und_risk_str:>12}")

    # --- new: prepare CSV rows and save to output folder ---
    csv_rows = []
    start_of_year = datetime(now.year, 1, 1)
    for etf, underlying, avg_diff in avg_diffs:
        # get YTD and risk values already computed (recompute small pieces for CSV if needed)
        etf_ytd = get_ytd_return(etf)
        und_ytd = get_ytd_return(underlying)
        etf_risk, und_risk = risk_map.get(etf, (None, None))

        # fetch price history from start of year to now for start/current prices
        try:
            hist_etf = yf.Ticker(etf).history(start=start_of_year, end=now)
            if not hist_etf.empty:
                etf_start_price = hist_etf["Close"].iloc[0]
                etf_current_price = hist_etf["Close"].iloc[-1]
            else:
                etf_start_price = None
                # attempt to get most recent price
                recent = yf.Ticker(etf).history(period="7d")
                etf_current_price = recent["Close"].iloc[-1] if not recent.empty else None
        except Exception:
            etf_start_price = None
            etf_current_price = None

        try:
            hist_und = yf.Ticker(underlying).history(start=start_of_year, end=now)
            if not hist_und.empty:
                und_start_price = hist_und["Close"].iloc[0]
                und_current_price = hist_und["Close"].iloc[-1]
            else:
                und_start_price = None
                recent2 = yf.Ticker(underlying).history(period="7d")
                und_current_price = recent2["Close"].iloc[-1] if not recent2.empty else None
        except Exception:
            und_start_price = None
            und_current_price = None

        csv_rows.append({
            "ETF": etf,
            "Underlying": underlying,
            "Avg_Outperf_%": avg_diff,
            "ETF_YTD_%": etf_ytd,
            "Underlying_YTD_%": und_ytd,
            "ETF_Risk_%": etf_risk,
            "Underlying_Risk_%": und_risk,
            "ETF_StartOfYear": etf_start_price,
            "ETF_Current": etf_current_price,
            "Underlying_StartOfYear": und_start_price,
            "Underlying_Current": und_current_price
        })

    # build dataframe and save
    csv_df = pd.DataFrame(csv_rows)
    today_str = now.strftime("%Y%m%d")
    csv_path = os.path.join(OUTPUT_DIR, f"etfperf_{today_str}.csv")
    csv_df.to_csv(csv_path, index=False)
    print(f"\nETF performance CSV written to {csv_path}")

if __name__ == "__main__":
    analyze_leveraged_etfs()

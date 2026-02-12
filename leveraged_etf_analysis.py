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
    ("MEXX", "EWW"),    
    ("SPYU", "SPY"),
    ("BUZZ", "SPY"),
    ("KORU", "EWY")
    # Add more as needed
]

# Country ETFs
COUNTRY_ETFS = [
    "EWJ",   # iShares MSCI Japan ETF
    "EWG",   # iShares MSCI Germany ETF
    "EWU",   # iShares MSCI United Kingdom ETF
    "EWY",   # iShares MSCI South Korea ETF
    "EWC",   # iShares MSCI Canada ETF
    "EWA",   # iShares MSCI Australia ETF
    "EWZ",   # iShares MSCI Brazil ETF
    "EWH",   # iShares MSCI Hong Kong ETF
    "EWW",   # iShares MSCI Mexico ETF
    "EWI",   # iShares MSCI Italy ETF
    "EWP",   # iShares MSCI Spain ETF
    "EWQ",   # iShares MSCI France ETF
    "EWS",   # iShares MSCI Singapore ETF
    "EWT",   # iShares MSCI Taiwan ETF
    "EWL",   # iShares MSCI Switzerland ETF
    "INDA",  # iShares MSCI India ETF
    "EZA",   # iShares MSCI South Africa ETF
    "FXI",   # iShares China Large-Cap ETF
    "MCHI",  # iShares MSCI China ETF
    "EWN",   # iShares MSCI Netherlands ETF
    "THD",   # iShares MSCI Thailand ETF
    "EIDO",  # iShares MSCI Indonesia ETF
    "TUR",   # iShares MSCI Turkey ETF
    "EPOL",  # iShares MSCI Poland ETF
    "RSX",   # VanEck Russia ETF
    "ARGT",  # iShares MSCI Argentina ETF
    "ENOR",  # VanEck Norway ETF
]

# Sector ETFs
SECTOR_ETFS = [
    "XLK",   # Technology Select Sector SPDR Fund
    "XLF",   # Financial Select Sector SPDR Fund
    "XLE",   # Energy Select Sector SPDR Fund
    "XLV",   # Health Care Select Sector SPDR Fund
    "XLY",   # Consumer Discretionary Select Sector SPDR Fund
    "XLP",   # Consumer Staples Select Sector SPDR Fund
    "XLI",   # Industrial Select Sector SPDR Fund
    "XLU",   # Utilities Select Sector SPDR Fund
    "XLB",   # Materials Select Sector SPDR Fund
    "XLRE",  # Real Estate Select Sector SPDR Fund
    "XLC",   # Communication Services Select Sector SPDR Fund
    "VGT",   # Vanguard Information Technology ETF
    "VFH",   # Vanguard Financials ETF
    "VDE",   # Vanguard Energy ETF
    "VHT",   # Vanguard Health Care ETF
    "VAW",   # Vanguard Materials ETF
    "VNQ",   # Vanguard Real Estate ETF
    "VOX",   # Vanguard Communication Services ETF
    "VIS",   # Vanguard Industrials ETF
    "VPU",   # Vanguard Utilities ETF
    "VCR",   # Vanguard Consumer Discretionary ETF
    "VDC",   # Vanguard Consumer Staples ETF
    "SOXX",  # iShares Semiconductor ETF
    "XBI",   # SPDR S&P Biotech ETF
    "IBB",   # iShares Biotechnology ETF
    "IYR",   # iShares U.S. Real Estate ETF
    "KRE",   # SPDR S&P Regional Banking ETF
    "SMH",   # VanEck Semiconductor ETF
    "ITB",   # iShares U.S. Home Construction ETF
    "IYT",   # iShares Transportation Average ETF
    "OIH",   # VanEck Oil Services ETF
    "UFO",
]

# Subsector ETFs - More granular industry exposure
SUBSECTOR_ETFS = [
    # Technology Subsectors
    "IGV",   # iShares Expanded Tech-Software Sector ETF
    "CIBR",  # First Trust NASDAQ Cybersecurity ETF
    "HACK",  # ETFMG Prime Cyber Security ETF
    "SKYY",  # First Trust Cloud Computing ETF
    "CLOU",  # Global X Cloud Computing ETF
    "FINX",  # Global X FinTech ETF
    "ROBO",  # ROBO Global Robotics and Automation Index ETF
    "BOTZ",  # Global X Robotics & Artificial Intelligence ETF
    "ARKW",  # ARK Next Generation Internet ETF
    "ARKQ",  # ARK Autonomous Tech & Robotics ETF
    "WCLD",  # WisdomTree Cloud Computing Fund
    "QTUM",  # Defiance Quantum ETF
    "BLOK",  # Amplify Transformational Data Sharing ETF
    "METV",  # Roundhill Ball Metaverse ETF
    
    # Financial Subsectors
    "KBE",   # SPDR S&P Bank ETF
    "IAK",   # iShares U.S. Insurance ETF
    "KIE",   # SPDR S&P Insurance ETF
    "KBWB",  # Invesco KBW Bank ETF
    "KBWR",  # Invesco KBW Regional Banking ETF
    "KBWP",  # Invesco KBW Property & Casualty Insurance ETF
    "IAI",   # iShares U.S. Broker-Dealers & Securities Exchanges ETF
    
    # Healthcare Subsectors
    "IHE",   # iShares U.S. Pharmaceuticals ETF
    "PJP",   # Invesco Dynamic Pharmaceuticals ETF
    "IHI",   # iShares U.S. Medical Devices ETF
    "IHF",   # iShares U.S. Healthcare Providers ETF
    "XPH",   # SPDR S&P Pharmaceuticals ETF
    "GNOM",  # Global X Genomics & Biotechnology ETF
    "ARKG",  # ARK Genomic Revolution ETF
    "SBIO",  # ALPS Medical Breakthroughs ETF
    
    # Energy Subsectors
    "XOP",   # SPDR S&P Oil & Gas Exploration & Production ETF
    "IEO",   # iShares U.S. Oil & Gas Exploration & Production ETF
    "ICLN",  # iShares Global Clean Energy ETF
    "TAN",   # Invesco Solar ETF
    "FAN",   # First Trust Global Wind Energy ETF
    "URA",   # Global X Uranium ETF
    "URNM",  # Sprott Uranium Miners ETF
    "PBW",   # Invesco WilderHill Clean Energy ETF
    "QCLN",  # First Trust NASDAQ Clean Edge Green Energy Index Fund
    "ACES",  # ALPS Clean Energy ETF
    "GRID",  # First Trust NASDAQ Clean Edge Smart Grid Infrastructure Index Fund
    
    # Consumer Subsectors
    "XRT",   # SPDR S&P Retail ETF
    "XHB",   # SPDR S&P Homebuilders ETF
    "PEJ",   # Invesco Dynamic Leisure and Entertainment ETF
    "AWAY",  # ETFMG Travel Tech ETF
    "JETS",  # U.S. Global Jets ETF
    "ONLN",  # ProShares Online Retail ETF
    "CNRG",  # SPDR S&P Kensho Clean Power ETF
    "FDN",   # First Trust Dow Jones Internet Index Fund
    "GAMR",  # Wedbush ETFMG Video Game Tech ETF
    
    # Industrial Subsectors
    "XAR",   # SPDR S&P Aerospace & Defense ETF
    "ITA",   # iShares U.S. Aerospace & Defense ETF
    "PAVE",  # Global X U.S. Infrastructure Development ETF
    "PKB",   # Invesco Dynamic Building & Construction ETF
    "IDRV",  # iShares Self-Driving EV and Tech ETF
    "DRIV",  # Global X Autonomous & Electric Vehicles ETF
    "SNSR",  # Global X Internet of Things ETF
    
    # Materials Subsectors
    "GDX",   # VanEck Gold Miners ETF
    "GDXJ",  # VanEck Junior Gold Miners ETF
    "SIL",   # Global X Silver Miners ETF
    "PICK",  # iShares MSCI Global Metals & Mining Producers ETF
    "COPX",  # Global X Copper Miners ETF
    "LIT",   # Global X Lithium & Battery Tech ETF
    "REMX",  # VanEck Rare Earth/Strategic Metals ETF
    "WOOD",  # iShares Global Timber & Forestry ETF
    "MOO",   # VanEck Agribusiness ETF
    
    # Communication Services Subsectors
    "SOCL",  # Global X Social Media ETF
    "NXTG",  # First Trust Indxx NextG ETF
    "FIVG",  # Defiance Next Gen Connectivity ETF
    
    # Real Estate Subsectors
    "HOMZ",  # Hoya Capital Housing ETF
    "INDS",  # Pacer Benchmark Industrial Real Estate SCTR ETF
    "REET",  # iShares Global REIT ETF
    "SRVR",  # Pacer Benchmark Data & Infrastructure Real Estate SCTR ETF
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

def get_aum_from_yfinance(ticker):
    """
    Fetch AUM (Assets Under Management) from Yahoo Finance.
    Returns AUM in billions or None if unavailable.
    """
    try:
        etf = yf.Ticker(ticker)
        info = etf.info
        
        # Yahoo Finance stores AUM as 'totalAssets'
        total_assets = info.get('totalAssets', None)
        
        if total_assets and total_assets > 0:
            # Convert to billions
            return total_assets / 1_000_000_000
        return None
    except Exception as e:
        print(f"  Warning: Could not fetch AUM from Yahoo Finance for {ticker}: {e}")
        return None

def get_aum_from_fund_info(ticker):
    """
    Fetch AUM from additional fund information sources.
    This can be expanded to include other data providers.
    Returns AUM in billions or None if unavailable.
    """
    try:
        etf = yf.Ticker(ticker)
        
        # Try to get from fund profile or other attributes
        # Some ETFs have 'nav' (Net Asset Value) information
        fund_data = etf.funds_data
        if fund_data is not None and hasattr(fund_data, 'total_net_assets'):
            assets = fund_data.total_net_assets
            if assets and assets > 0:
                return assets / 1_000_000_000
        
        return None
    except Exception as e:
        # Silently fail as this is a secondary source
        return None

def get_aum_multiple_sources(ticker):
    """
    Fetch AUM from multiple sources and return the highest value.
    Returns a tuple: (AUM in billions, source name)
    """
    aum_values = {}
    
    # Source 1: Yahoo Finance
    yf_aum = get_aum_from_yfinance(ticker)
    if yf_aum is not None:
        aum_values['Yahoo Finance'] = yf_aum
    
    # Source 2: Fund Info (alternative method)
    fund_aum = get_aum_from_fund_info(ticker)
    if fund_aum is not None:
        aum_values['Fund Info'] = fund_aum
    
    # Return the highest AUM value
    if aum_values:
        max_source = max(aum_values, key=aum_values.get)
        max_aum = aum_values[max_source]
        return max_aum, max_source
    
    return None, None

def format_aum(aum_billions):
    """
    Format AUM for display.
    Returns formatted string like "$1.23B" or None if AUM is unavailable.
    """
    if aum_billions is None:
        return None
    
    if aum_billions >= 1000:
        # Display in trillions
        return f"${aum_billions/1000:.2f}T"
    elif aum_billions >= 1:
        # Display in billions
        return f"${aum_billions:.2f}B"
    else:
        # Display in millions
        return f"${aum_billions*1000:.0f}M"

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

def get_etf_performance(ticker, days):
    """
    Get the performance of an ETF over a given number of days.
    Returns percentage change or None if data unavailable.
    """
    now = datetime.now()
    start = now - timedelta(days=days)
    try:
        data = yf.Ticker(ticker).history(start=start, end=now)
        if data.empty or len(data) < 2:
            return None
        start_price = data["Close"].iloc[0]
        end_price = data["Close"].iloc[-1]
        return (end_price - start_price) / start_price * 100
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

def get_etf_name(ticker):
    """
    Get the long name of an ETF.
    """
    try:
        info = yf.Ticker(ticker).info
        return info.get('longName', ticker)
    except:
        return ticker

def calculate_trend_sma(ticker):
    """
    Determine trend using Simple Moving Averages (50-day and 200-day).
    Returns: 'Uptrend', 'Downtrend', 'Sideways', or 'Unknown'
    """
    try:
        now = datetime.now()
        start = now - timedelta(days=250)  # Get enough data for 200-day SMA
        data = yf.Ticker(ticker).history(start=start, end=now)
        
        if data.empty or len(data) < 50:
            return "Unknown"
        
        # Calculate SMAs
        sma_50 = data["Close"].rolling(window=50).mean()
        sma_200 = data["Close"].rolling(window=200).mean()
        
        current_price = data["Close"].iloc[-1]
        
        # Check if we have enough data
        if pd.isna(sma_50.iloc[-1]):
            return "Unknown"
        
        # Determine trend based on price position relative to SMAs
        above_50 = current_price > sma_50.iloc[-1]
        
        if len(data) >= 200 and not pd.isna(sma_200.iloc[-1]):
            above_200 = current_price > sma_200.iloc[-1]
            if above_50 and above_200:
                return "Uptrend"
            elif not above_50 and not above_200:
                return "Downtrend"
            else:
                return "Sideways"
        else:
            # Only have 50-day SMA
            if above_50:
                return "Uptrend"
            else:
                return "Downtrend"
    except Exception as e:
        return "Unknown"

def calculate_trend_ma_crossover(ticker):
    """
    Determine trend using Moving Average Crossover (50-day vs 200-day).
    Golden Cross = Uptrend, Death Cross = Downtrend
    Returns: 'Uptrend', 'Downtrend', 'Sideways', or 'Unknown'
    """
    try:
        now = datetime.now()
        start = now - timedelta(days=250)
        data = yf.Ticker(ticker).history(start=start, end=now)
        
        if data.empty or len(data) < 200:
            return "Unknown"
        
        sma_50 = data["Close"].rolling(window=50).mean()
        sma_200 = data["Close"].rolling(window=200).mean()
        
        if pd.isna(sma_50.iloc[-1]) or pd.isna(sma_200.iloc[-1]):
            return "Unknown"
        
        # Golden Cross: 50-day > 200-day = Uptrend
        # Death Cross: 50-day < 200-day = Downtrend
        diff = sma_50.iloc[-1] - sma_200.iloc[-1]
        diff_pct = (diff / sma_200.iloc[-1]) * 100
        
        if diff_pct > 2:  # 50-day significantly above 200-day
            return "Uptrend"
        elif diff_pct < -2:  # 50-day significantly below 200-day
            return "Downtrend"
        else:
            return "Sideways"
    except Exception as e:
        return "Unknown"

def calculate_trend_linear_regression(ticker, days=30):
    """
    Determine trend using linear regression slope over recent period.
    Returns: 'Uptrend', 'Downtrend', 'Sideways', or 'Unknown'
    """
    try:
        now = datetime.now()
        start = now - timedelta(days=days + 10)  # Extra days for safety
        data = yf.Ticker(ticker).history(start=start, end=now)
        
        if data.empty or len(data) < 10:
            return "Unknown"
        
        # Take last 'days' data points
        recent_data = data["Close"].tail(min(days, len(data)))
        
        # Perform linear regression
        x = np.arange(len(recent_data))
        y = recent_data.values
        
        # Calculate slope
        coefficients = np.polyfit(x, y, 1)
        slope = coefficients[0]
        
        # Normalize slope by average price
        avg_price = np.mean(y)
        normalized_slope = (slope / avg_price) * 100  # Slope as percentage of price
        
        # Determine trend based on slope
        if normalized_slope > 0.1:  # Positive slope
            return "Uptrend"
        elif normalized_slope < -0.1:  # Negative slope
            return "Downtrend"
        else:
            return "Sideways"
    except Exception as e:
        return "Unknown"

def calculate_trend_momentum(ticker):
    """
    Determine trend using price momentum (comparing recent periods).
    Returns: 'Uptrend', 'Downtrend', 'Sideways', or 'Unknown'
    """
    try:
        now = datetime.now()
        start = now - timedelta(days=90)
        data = yf.Ticker(ticker).history(start=start, end=now)
        
        if data.empty or len(data) < 30:
            return "Unknown"
        
        # Compare recent 2 weeks vs previous 2 weeks
        recent_14d = data["Close"].tail(14).mean()
        prev_14d = data["Close"].tail(28).head(14).mean()
        
        if pd.isna(recent_14d) or pd.isna(prev_14d):
            return "Unknown"
        
        change_pct = ((recent_14d - prev_14d) / prev_14d) * 100
        
        if change_pct > 2:
            return "Uptrend"
        elif change_pct < -2:
            return "Downtrend"
        else:
            return "Sideways"
    except Exception as e:
        return "Unknown"

def analyze_all_etfs():
    """
    Analyze leveraged, country, sector, and subsector ETFs for various performance periods.
    Output results to recent-etf-performance-{ddmmyyyy}.csv
    """
    now = datetime.now()
    date_str = now.strftime("%d%m%Y")
    
    all_results = []
    
    def analyze_etf(ticker, group):
        """Helper function to analyze a single ETF with all metrics"""
        print(f"  Analyzing {ticker}...")
        
        # Get performance metrics
        week_perf = get_etf_performance(ticker, 7)
        month_perf = get_etf_performance(ticker, 30)
        six_month_perf = get_etf_performance(ticker, 182)
        ytd_perf = get_ytd_return(ticker)
        
        # Get ETF name
        etf_name = get_etf_name(ticker)
        
        # Get AUM from multiple sources
        aum_billions, aum_source = get_aum_multiple_sources(ticker)
        aum_formatted = format_aum(aum_billions)
        
        if aum_billions:
            print(f"    AUM: {aum_formatted} (from {aum_source})")
        
        # Calculate trend indicators
        trend_sma = calculate_trend_sma(ticker)
        trend_ma_cross = calculate_trend_ma_crossover(ticker)
        trend_regression = calculate_trend_linear_regression(ticker)
        trend_momentum = calculate_trend_momentum(ticker)
        
        return {
            "ETF_Ticker": ticker,
            "ETF_Name": etf_name,
            "Group": group,
            "AUM": aum_formatted,
            "AUM_Billions": aum_billions,
            "1_Week_Performance_%": week_perf,
            "1_Month_Performance_%": month_perf,
            "6_Month_Performance_%": six_month_perf,
            "YTD_Performance_%": ytd_perf,
            "Trend_SMA": trend_sma,
            "Trend_MA_Crossover": trend_ma_cross,
            "Trend_Linear_Regression": trend_regression,
            "Trend_Momentum": trend_momentum
        }
    
    print("Analyzing Leveraged ETFs...")
    # Analyze leveraged ETFs
    for etf, underlying in LEVERAGED_ETFS:
        result = analyze_etf(etf, "Leveraged")
        all_results.append(result)
    
    print("\nAnalyzing Country ETFs...")
    # Analyze country ETFs
    for etf in COUNTRY_ETFS:
        result = analyze_etf(etf, "Country")
        all_results.append(result)
    
    print("\nAnalyzing Sector ETFs...")
    # Analyze sector ETFs
    for etf in SECTOR_ETFS:
        result = analyze_etf(etf, "Sector")
        all_results.append(result)
    
    print("\nAnalyzing Subsector ETFs...")
    # Analyze subsector ETFs
    for etf in SUBSECTOR_ETFS:
        result = analyze_etf(etf, "Subsector")
        all_results.append(result)
    
    # Create DataFrame and sort
    df = pd.DataFrame(all_results)
    
    # Sort by 1-week performance (descending), then 1-month performance (descending)
    df_sorted = df.sort_values(
        by=["1_Week_Performance_%", "1_Month_Performance_%"],
        ascending=[False, False],
        na_position='last'
    )
    
    # Save to CSV
    output_filename = f"recent-etf-performance-{date_str}.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    df_sorted.to_csv(output_path, index=False)
    
    print(f"\n{'='*80}")
    print(f"ETF Performance Analysis Complete!")
    print(f"Results saved to: {output_path}")
    print(f"Total ETFs analyzed: {len(all_results)}")
    print(f"{'='*80}")
    
    # Display top 10 performers
    print("\nTop 10 ETFs by 1-Week Performance:")
    print(f"{'Ticker':<8} {'Group':<12} {'AUM':>12} {'1-Week %':>12} {'1-Month %':>12} {'6-Month %':>12} {'YTD %':>12} {'Trend (SMA)':<12}")
    print("-" * 100)
    for idx, row in df_sorted.head(10).iterrows():
        aum_str = row['AUM'] if pd.notna(row['AUM']) and row['AUM'] else "        N/A"
        week_str = f"{row['1_Week_Performance_%']:10.2f}" if pd.notna(row['1_Week_Performance_%']) else "       N/A"
        month_str = f"{row['1_Month_Performance_%']:10.2f}" if pd.notna(row['1_Month_Performance_%']) else "       N/A"
        six_month_str = f"{row['6_Month_Performance_%']:10.2f}" if pd.notna(row['6_Month_Performance_%']) else "       N/A"
        ytd_str = f"{row['YTD_Performance_%']:10.2f}" if pd.notna(row['YTD_Performance_%']) else "       N/A"
        trend_str = str(row['Trend_SMA'])
        print(f"{row['ETF_Ticker']:<8} {row['Group']:<12} {aum_str:>12} {week_str:>12} {month_str:>12} {six_month_str:>12} {ytd_str:>12} {trend_str:<12}")
    
    return output_path

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
    import sys
    
    # Check if user wants the new analysis or old leveraged ETF analysis
    if len(sys.argv) > 1 and sys.argv[1] == "--legacy":
        print("Running legacy leveraged ETF analysis...")
        analyze_leveraged_etfs()
    else:
        print("Running comprehensive ETF analysis (Leveraged, Country, Sector)...")
        analyze_all_etfs()


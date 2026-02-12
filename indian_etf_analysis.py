import json
import os
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

# Indian ETFs (NSE listings, use .NS suffix). Grouped for clarity.
INDIA_ETFS = {
    "Broad Market": [
        "NIFTYBEES.NS",
        "JUNIORBEES.NS",
        "NIFTY1.NS",
        "QNIFTY.NS",
        "IVZINNIFTY.NS",
        "BSLNIFTY.NS",
        "NIFTYIETF.NS",
        "SETFNIF50.NS",
        "LICNETFN50.NS",
        "AXISNIFTY.NS",
        "NETF.NS",
        "NIFTYETF.NS",
        "NIFTYADD.NS",
        "NIFTYBETF.NS",
        "HDFCNIFTY.NS",
        "NIF100BEES.NS",
        "NIF100IETF.NS",
        "HDFCNIF100.NS",
        "LICNFNHGP.NS",
        "BSE500IETF.NS",
        "HDFCBSE500.NS",
        "MONIFTY500.NS",
        "SENSEXIETF.NS",
        "HDFCSENSEX.NS",
        "LICNETFSEN.NS",
        "AXSENSEX.NS",
        "BSLSENETFG.NS",
        "SENSEXADD.NS",
    ],
    "Mid/Small": [
        "MID150BEES.NS",
        "MIDCAPIETF.NS",
        "MIDCAPETF.NS",
        "HDFCMID150.NS",
        "MIDCAP.NS",
        "MOSMALL250.NS",
        "SMALLCAP.NS",
        "MIDSMALL.NS",
        "LICNMID100.NS",
        "MIDQ50ADD.NS",
        "MIDCAPBETA.NS",
        "MOM100.NS",
        "NEXT50.NS",
        "NEXT50IETF.NS",
        "HDFCNEXT50.NS",
        "ABSLNN50ET.NS",
        "SETFNN50.NS",
    ],
    "Sector": [
        "BANKBEES.NS",
        "PSUBNKBEES.NS",
        "PSUBNKIETF.NS",
        "PSUBANKADD.NS",
        "BANKADD.NS",
        "BANKETF.NS",
        "BANKIETF.NS",
        "AXISBNKETF.NS",
        "HDFCPVTBAN.NS",
        "PVTBANIETF.NS",
        "PVTBANKADD.NS",
        "SBIETFPB.NS",
        "FINIETF.NS",
        "BFSI.NS",
        "FMCGIETF.NS",
        "AUTOIETF.NS",
        "AUTOBEES.NS",
        "CONSUMBEES.NS",
        "SBIETFCON.NS",
        "AXISCETF.NS",
        "CONSUMIETF.NS",
        "INFRAIETF.NS",
        "ITBEES.NS",
        "IT.NS",
        "ITADD.NS",
        "ITETF.NS",
        "AXISTECETF.NS",
        "PHARMABEES.NS",
        "HEALTHIETF.NS",
        "AXISHCETF.NS",
        "HEALTHY.NS",
        "MOHEALTH.NS",
        "HEALTHADD.NS",
        "HDFCPSUBK.NS",
    ],
    "Thematic": [
        "CPSEETF.NS",
        "ABSLPSE.NS",
        "SHARIABEES.NS",
        "MAKEINDIA.NS",
        "MNC.NS",
        "MAFANG.NS",
    ],
    "Factor": [
        "NV20BEES.NS",
        "NV20.NS",
        "NV20IETF.NS",
        "NIFTYBETA.NS",
        "SENSEXBETA.NS",
        "ALPHA.NS",
        "ALPHAETF.NS",
        "MOMOMENTUM.NS",
        "MOM30IETF.NS",
        "MOMENTUM.NS",
        "LOWVOLIETF.NS",
        "LOWVOL.NS",
        "LOWVOL1.NS",
        "MOLOWVOL.NS",
        "HDFCLOWVOL.NS",
        "HDFCMOMENT.NS",
        "HDFCQUAL.NS",
        "NIFTYQLITY.NS",
        "SBIETFQLTY.NS",
        "QUAL30IETF.NS",
        "EQUAL50ADD.NS",
        "DIVOPPBEES.NS",
        "HDFCVALUE.NS",
        "MOVALUE.NS",
        "MOQUALITY.NS",
        "HDFCGROWTH.NS",
    ],
    "Commodity": [
        "GOLDBEES.NS",
        "GOLDBETA.NS",
        "GOLD1.NS",
        "SETFGOLD.NS",
        "IVZINGOLD.NS",
        "HDFCGOLD.NS",
        "GOLDIETF.NS",
        "AXISGOLD.NS",
        "BSLGOLDETF.NS",
        "LICMFGOLD.NS",
        "GOLDETF.NS",
        "GOLDADD.NS",
        "EGOLD.NS",
        "BBNPPGOLD.NS",
        "TATAGOLD.NS",
        "GOLDCASE.NS",
        "SILVERBEES.NS",
        "SILVERIETF.NS",
        "SILVER.NS",
        "SILVERADD.NS",
        "SILVER1.NS",
        "SILVERBETA.NS",
        "SILVERAG.NS",
        "ESILVER.NS",
        "TATSILV.NS",
        "AXISILVER.NS",
        "HDFCSILVER.NS",
    ],
    "International": [
        "MON100.NS",
        "MONQ50.NS",
        "MASPTOP50.NS",
        "MAHKTECH.NS",
        "HNGSNGBEES.NS",
    ],
    "Fixed Income/Liquid": [
        "LIQUIDBEES.NS",
        "LIQUIDETF.NS",
        "LIQUIDIETF.NS",
        "LIQUID1.NS",
        "LIQUID.NS",
        "LIQUIDSBI.NS",
        "LIQUIDCASE.NS",
        "LIQUIDADD.NS",
        "LIQUIDBETF.NS",
        "ABSLLIQUID.NS",
        "HDFCLIQUID.NS",
        "LICNETFGSC.NS",
        "SETF10GILT.NS",
        "MOGSEC.NS",
        "SDL26BEES.NS",
        "GILT5YBEES.NS",
        "GSEC5IETF.NS",
        "GSEC10IETF.NS",
        "GSEC10YEAR.NS",
        "GILT10BETA.NS",
        "GILT5BETA.NS",
        "AXISBPSETF.NS",
        "EBBETF0430.NS",
        "EBBETF0431.NS",
        "BBETF0432.NS",
        "EBBETF0433.NS",
    ],
}

PERIODS = [
    ("1 Week", 7),
    ("1 Month", 30),
    ("6 Months", 182),
    ("YTD", None),
    ("1 Year", 365),
]

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
EXCLUSION_FILE = os.path.join(OUTPUT_DIR, "india_etf_exclusions.json")

ERROR_FLAGS = {}
INFO_CACHE = {}

INDEX_PATTERNS = [
    (r"\bnifty\s*50\b", "nifty 50"),
    (r"\bnifty\s*next\s*50\b", "nifty next 50"),
    (r"\bnifty\s*100\b", "nifty 100"),
    (r"\bnifty\s*200\b", "nifty 200"),
    (r"\bnifty\s*500\b", "nifty 500"),
    (r"\bsensex\b", "sensex"),
    (r"\bnifty\s*midcap\s*150\b", "nifty midcap 150"),
    (r"\bnifty\s*midcap\s*100\b", "nifty midcap 100"),
    (r"\bnifty\s*midcap\s*50\b", "nifty midcap 50"),
    (r"\bnifty\s*smallcap\s*250\b", "nifty smallcap 250"),
    (r"\bnifty\s*smallcap\s*100\b", "nifty smallcap 100"),
    (r"\bnifty\s*smallcap\s*50\b", "nifty smallcap 50"),
    (r"\bnifty\s*bank\b|\bbank\s*nifty\b", "nifty bank"),
    (r"\bnifty\s*psu\s*bank\b", "nifty psu bank"),
    (r"\bnifty\s*pvt\s*bank\b|\bnifty\s*private\s*bank\b", "nifty private bank"),
    (r"\bnifty\s*it\b", "nifty it"),
    (r"\bnifty\s*fmcg\b", "nifty fmcg"),
    (r"\bnifty\s*auto\b", "nifty auto"),
    (r"\bnifty\s*infra\b", "nifty infra"),
    (r"\bnifty\s*pharma\b", "nifty pharma"),
    (r"\bnifty\s*health(\s*care)?\b", "nifty healthcare"),
    (r"\bnifty\s*consumption\b", "nifty consumption"),
    (r"\bnifty\s*financial\s*services\b", "nifty financial services"),
    (r"\bnifty\s*alpha\b", "nifty alpha"),
    (r"\bnifty\s*low\s*vol(atility)?\b", "nifty low volatility"),
    (r"\bnifty\s*momentum\b", "nifty momentum"),
    (r"\bnifty\s*quality\b", "nifty quality"),
    (r"\bnifty\s*value\b", "nifty value"),
    (r"\bnifty\s*equal\s*weight\s*50\b", "nifty equal weight 50"),
    (r"\bnifty\s*dividend\b", "nifty dividend"),
    (r"\bnifty\s*cpse\b", "nifty cpse"),
    (r"\bgold\b", "gold"),
    (r"\bsilver\b", "silver"),
    (r"\bliquid\b", "liquid"),
    (r"\bgsec\b|\bgilt\b", "gsec/gilt"),
    (r"\bsdl\b", "state development loan"),
]


def _history_from_yfinance(ticker, start, end):
    try:
        data = yf.Ticker(ticker).history(start=start, end=end)
        return data if not data.empty else None
    except Exception as exc:
        message = str(exc)
        if "404" in message:
            flag_ticker(ticker, "http_404")
        return None


def _history_from_nsepython(ticker, start, end):
    """
    Optional fallback: uses nsepython if installed.
    Returns a DataFrame with a "Close" column or None.
    """
    try:
        from nsepython import equity_history  # type: ignore

        symbol = ticker.replace(".NS", "")
        start_str = start.strftime("%d-%m-%Y")
        end_str = end.strftime("%d-%m-%Y")
        raw = equity_history(symbol, "EQ", start_str, end_str)
        if raw is None or raw.empty:
            return None
        df = raw.rename(columns={"CH_CLOSE": "Close"})
        df["Close"] = pd.to_numeric(df["Close"], errors="coerce")
        df = df.dropna(subset=["Close"])
        return df if not df.empty else None
    except Exception:
        return None


def fetch_price_history(ticker, start, end):
    data = _history_from_yfinance(ticker, start, end)
    if data is not None:
        return data
    return _history_from_nsepython(ticker, start, end)


def flag_ticker(ticker, reason):
    ERROR_FLAGS.setdefault(ticker, set()).add(reason)


def has_recent_history(ticker, days=60):
    now = datetime.now()
    start = now - timedelta(days=days)
    data = fetch_price_history(ticker, start, now)
    if data is None or len(data) < 2:
        flag_ticker(ticker, "empty_history")
        return False
    return True


def load_exclusions():
    if not os.path.exists(EXCLUSION_FILE):
        return {}
    try:
        with open(EXCLUSION_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload.get("exclusions", {})
    except Exception:
        return {}


def save_exclusions(exclusions):
    payload = {
        "updated_at": datetime.now().strftime("%Y-%m-%d"),
        "exclusions": exclusions,
    }
    with open(EXCLUSION_FILE, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def get_return(ticker, start, end):
    data = fetch_price_history(ticker, start, end)
    if data is None or len(data) < 2:
        return None
    start_price = data["Close"].iloc[0]
    end_price = data["Close"].iloc[-1]
    return (end_price - start_price) / start_price * 100


def get_ytd_return(ticker):
    now = datetime.now()
    start = datetime(now.year, 1, 1)
    return get_return(ticker, start, now)


def get_annualized_std(ticker, period_days=365):
    now = datetime.now()
    start = now - timedelta(days=period_days)
    data = fetch_price_history(ticker, start, now)
    if data is None or len(data) < 2:
        return None
    returns = data["Close"].pct_change().dropna()
    if returns.empty:
        return None
    return returns.std() * np.sqrt(252) * 100


def get_etf_name(ticker):
    return get_etf_info(ticker)["name"]


def infer_index_key(etf_name):
    if not etf_name:
        return None
    name = etf_name.lower()
    for pattern, key in INDEX_PATTERNS:
        if re.search(pattern, name):
            return key
    return None


def get_etf_info(ticker):
    cached = INFO_CACHE.get(ticker)
    if cached is not None:
        return cached
    info = {}
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        info = {}
    name = info.get("longName") or info.get("shortName") or ticker
    aum = info.get("totalAssets")
    if isinstance(aum, (int, float)) and np.isnan(aum):
        aum = None
    result = {
        "name": name,
        "aum": aum,
        "index_key": infer_index_key(name),
    }
    INFO_CACHE[ticker] = result
    return result


def select_primary_tickers(tickers):
    selected = {}
    for ticker in tickers:
        info = get_etf_info(ticker)
        index_key = info["index_key"] or ticker
        existing = selected.get(index_key)
        if existing is None:
            selected[index_key] = ticker
            continue
        current_aum = get_etf_info(existing)["aum"]
        candidate_aum = info["aum"]
        if current_aum is None and candidate_aum is None:
            continue
        if current_aum is None and candidate_aum is not None:
            selected[index_key] = ticker
            continue
        if candidate_aum is None and current_aum is not None:
            continue
        if candidate_aum > current_aum:
            selected[index_key] = ticker
    return list(selected.values())


def calculate_trend_sma(ticker):
    try:
        now = datetime.now()
        start = now - timedelta(days=250)
        data = fetch_price_history(ticker, start, now)
        if data is None or len(data) < 50:
            return "Unknown"
        sma_50 = data["Close"].rolling(window=50).mean()
        sma_200 = data["Close"].rolling(window=200).mean()
        current_price = data["Close"].iloc[-1]
        if pd.isna(sma_50.iloc[-1]):
            return "Unknown"
        above_50 = current_price > sma_50.iloc[-1]
        if len(data) >= 200 and not pd.isna(sma_200.iloc[-1]):
            above_200 = current_price > sma_200.iloc[-1]
            if above_50 and above_200:
                return "Uptrend"
            if not above_50 and not above_200:
                return "Downtrend"
            return "Sideways"
        return "Uptrend" if above_50 else "Downtrend"
    except Exception:
        return "Unknown"


def analyze_indian_etfs():
    now = datetime.now()
    date_str = now.strftime("%d%m%Y")

    all_results = []
    exclusions = load_exclusions()

    for group, tickers in INDIA_ETFS.items():
        print(f"Analyzing {group} ETFs...")
        for ticker in select_primary_tickers(tickers):
            if ticker in exclusions:
                continue
            if not has_recent_history(ticker):
                continue
            print(f"  Analyzing {ticker}...")
            week_perf = get_return(ticker, now - timedelta(days=7), now)
            month_perf = get_return(ticker, now - timedelta(days=30), now)
            six_month_perf = get_return(ticker, now - timedelta(days=182), now)
            ytd_perf = get_ytd_return(ticker)
            etf_name = get_etf_name(ticker)
            trend_sma = calculate_trend_sma(ticker)
            risk = get_annualized_std(ticker)

            all_results.append(
                {
                    "ETF_Ticker": ticker,
                    "ETF_Name": etf_name,
                    "Group": group,
                    "1_Week_Performance_%": round_value(week_perf),
                    "1_Month_Performance_%": round_value(month_perf),
                    "6_Month_Performance_%": round_value(six_month_perf),
                    "YTD_Performance_%": round_value(ytd_perf),
                    "Risk_StdDev_%": round_value(risk),
                    "Trend_SMA": trend_sma,
                }
            )

    df = pd.DataFrame(all_results)
    df_sorted = df.sort_values(
        by=["1_Week_Performance_%", "1_Month_Performance_%"],
        ascending=[False, False],
        na_position="last",
    )

    print_group_summary(df_sorted)
    print_qc_summary(df_sorted)

    output_filename = f"india_etf_performance_{date_str}.csv"
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    output_path = write_csv_safely(df_sorted, output_path)

    print("\n" + "=" * 80)
    print("India ETF Performance Analysis Complete!")
    print(f"Results saved to: {output_path}")
    print(f"Total ETFs analyzed: {len(all_results)}")
    if ERROR_FLAGS:
        for ticker, reasons in ERROR_FLAGS.items():
            existing = exclusions.get(ticker, [])
            merged = sorted(set(existing + list(reasons)))
            exclusions[ticker] = merged
        save_exclusions(exclusions)
        print(f"Exclusions updated: {EXCLUSION_FILE}")
    print("=" * 80)

    return output_path


def write_csv_safely(df, output_path):
    """
    Write CSV, and if the target is locked/open, retry with a timestamped name.
    """
    try:
        df.to_csv(output_path, index=False)
        return output_path
    except PermissionError:
        base, ext = os.path.splitext(output_path)
        fallback = f"{base}_{datetime.now().strftime('%H%M%S')}{ext}"
        df.to_csv(fallback, index=False)
        return fallback


def round_value(value, digits=2):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return value
    return round(value, digits)


def print_group_summary(df):
    if df.empty:
        return
    print("\nTop/Bottom performers by group (1-Week %):")
    groups = df.groupby("Group", dropna=False)
    for group, group_df in groups:
        valid = group_df.dropna(subset=["1_Week_Performance_%"])
        if valid.empty:
            continue
        top_row = valid.sort_values("1_Week_Performance_%", ascending=False).iloc[0]
        bottom_row = valid.sort_values("1_Week_Performance_%", ascending=True).iloc[0]
        print(
            f"  {group}: Top {top_row['ETF_Ticker']} ({top_row['1_Week_Performance_%']}%), "
            f"Bottom {bottom_row['ETF_Ticker']} ({bottom_row['1_Week_Performance_%']}%)"
        )


def print_qc_summary(df):
    if df.empty:
        return
    numeric_cols = [
        "1_Week_Performance_%",
        "1_Month_Performance_%",
        "6_Month_Performance_%",
        "YTD_Performance_%",
        "Risk_StdDev_%",
    ]
    missing_counts = df[numeric_cols].isna().sum().to_dict()
    print("\nQC summary:")
    print(f"  Missing values: {missing_counts}")
    movers = df.dropna(subset=["1_Week_Performance_%"]).sort_values(
        "1_Week_Performance_%", ascending=False
    )
    if not movers.empty:
        top = movers.head(5)[["ETF_Ticker", "1_Week_Performance_%"]].values.tolist()
        bottom = movers.tail(5)[["ETF_Ticker", "1_Week_Performance_%"]].values.tolist()
        print(f"  Top 5 (1-Week %): {top}")
        print(f"  Bottom 5 (1-Week %): {bottom}")


if __name__ == "__main__":
    analyze_indian_etfs()

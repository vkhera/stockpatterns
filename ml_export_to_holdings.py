import csv
import re
import yfinance as yf
from datetime import datetime, timedelta
import numpy as np
from scipy.stats import linregress

input_file = "ML_ExportData.csv"
output_file = "holdings_cleaned.csv"

def extract_export_datetime(row):
    # Example: 'Exported on: 06/26/2025 02:08 PM ET'
    match = re.search(r"Exported on:\s*([0-9/]+ [0-9:]+ [APM]+)", row)
    if match:
        return match.group(1)
    return ""

def extract_ticker(security_desc):
    # Ticker is the first word in the Security Description
    return security_desc.split()[0] if security_desc else ""

def get_beta(ticker):
    try:
        info = yf.Ticker(ticker).info
        beta = info.get("beta", "")
        return round(beta, 3) if beta is not None else ""
    except Exception:
        return ""

def get_trend(ticker):
    try:
        hist = yf.Ticker(ticker).history(period='30d')['Close']
        if len(hist) >= 15:
            from scipy.stats import linregress
            x = range(len(hist))
            y = hist.values
            slope, _, _, _, _ = linregress(x, y)
            threshold = 0.001 * y[0]  # 0.1% of starting price per day
            if slope > threshold:
                return 'uptrend'
            elif slope < -threshold:
                return 'downtrend'
            else:
                return 'sideways'
        else:
            print(f"[DEBUG] Not enough data for trend analysis for {ticker}, closes found: {len(hist)}")
            return ''
    except Exception as e:
        print(f"[DEBUG] Exception for {ticker}: {e}")
        return ""

def is_data_row(row):
    # Data rows have a non-empty Symbol Description and Quantity fields, and are not summary or interest rows
    return (
        len(row) > 2
        and row[1].strip() != ""
        and row[2].strip() != ""
        and "Cumulative Investment Return" not in row[6]
        and "Accrued Interest" not in row[4]
        and bool(row[1].strip().split()[0])
    )

def main():
    with open(input_file, newline='', encoding='utf-8-sig') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        export_datetime = ""
        header_row = None
        header_written = False

        for idx, row in enumerate(reader):
            if idx == 0:
                export_datetime = extract_export_datetime(row[0])
            # Find the header row (contains "Symbol Description")
            if not header_row and len(row) > 1 and "Symbol Description" in row[1]:
                header_row = row
                continue
            if is_data_row(row):
                if not header_written:
                    # Write header: Ticker, Symbol Description, rest of columns (excluding Symbol Description), Beta, Trend, Exported On
                    new_header = (
                        ["Ticker", "Symbol Description"] +
                        [col.strip() for i, col in enumerate(header_row) if i != 1] +
                        ["Beta", "Trend", "Exported On"]
                    )
                    writer.writerow(new_header)
                    header_written = True
                ticker = extract_ticker(row[1])
                beta = get_beta(ticker)
                trend = get_trend(ticker)
                new_row = (
                    [ticker, row[1].strip()] +
                    [col.strip() for i, col in enumerate(row) if i != 1] +
                    [beta, trend, export_datetime]
                )
                writer.writerow(new_row)

if __name__ == "__main__":
    main()
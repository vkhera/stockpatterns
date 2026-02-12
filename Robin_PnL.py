import pandas as pd
import os
from datetime import datetime, timedelta
from collections import defaultdict
import sys

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')
ACTIVITY_CSV = os.path.join(OUTPUT_DIR, 'Robin_Activity.csv')
LOG_FILE = os.path.join(OUTPUT_DIR, 'RobinPL.log')

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

def clean_currency(value):
    """Remove currency symbols and convert to float"""
    if pd.isna(value):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    # Remove $, commas, and whitespace
    cleaned = str(value).replace('$', '').replace(',', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def calculate_pnl():
    """Calculate short-term and long-term P&L from Robin_Activity.csv"""
    
    # Setup logging
    log_file = open(LOG_FILE, 'a', encoding='utf-8')
    log_file.write(f"\n{'='*80}\nP&L Calculation Run: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'='*80}\n")
    sys.stdout = TeeOutput(sys.stdout, log_file)
    
    try:
        print(f"Reading activity from: {ACTIVITY_CSV}")
        
        # Try different encodings and error handling
        encodings = ['utf-8', 'latin1', 'cp1252', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                print(f"Trying encoding: {encoding}")
                df = pd.read_csv(
                    ACTIVITY_CSV, 
                    encoding=encoding,
                    low_memory=False,
                    skip_blank_lines=True,
                    on_bad_lines='skip'  # Skip malformed rows
                )
                print(f"Successfully read CSV with {encoding} encoding")
                print(f"Total rows: {len(df)}")
                print(f"Columns: {list(df.columns)}")
                break
            except Exception as e:
                print(f"Failed with {encoding}: {e}")
                continue
        
        if df is None:
            print("Error: Could not read CSV with any encoding")
            return
        
        # Convert date column (adjust column name based on actual CSV)
        date_col = None
        for col in ['Activity Date', 'Date', 'Trans Date', 'Transaction Date', 'date']:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            print("Error: Could not find date column in CSV")
            return
        
        df[date_col] = pd.to_datetime(df[date_col])
        
        # Filter for current year
        current_year = datetime.now().year
        df_year = df[df[date_col].dt.year == current_year].copy()
        
        print(f"\nTotal transactions in {current_year}: {len(df_year)}")
        
        # Identify transaction type column
        type_col = None
        for col in ['Trans Code', 'Type', 'Trans Type', 'Transaction Type', 'Side', 'Description']:
            if col in df.columns:
                type_col = col
                break
        
        # Identify symbol column
        symbol_col = None
        for col in ['Instrument', 'Symbol', 'Ticker', 'Stock']:
            if col in df.columns:
                symbol_col = col
                break
        
        # Identify quantity and price columns
        qty_col = None
        for col in ['Quantity', 'Qty', 'Shares', 'Amount']:
            if col in df.columns:
                qty_col = col
                break
        
        price_col = None
        for col in ['Price', 'Unit Price', 'Share Price', 'Average Price']:
            if col in df.columns:
                price_col = col
                break
        
        print(f"Using columns - Date: {date_col}, Type: {type_col}, Symbol: {symbol_col}, Qty: {qty_col}, Price: {price_col}")
        
        # Separate buys and sells - adjust for Trans Code column
        if type_col == 'Trans Code':
            # Trans Code might have codes like 'Buy', 'Sell', 'B', 'S', etc.
            df_sells = df_year[df_year[type_col].str.contains('Sell|^S$|SLD', case=False, na=False, regex=True)].copy()
            df_buys = df[df[type_col].str.contains('Buy|^B$|BOT', case=False, na=False, regex=True)].copy()
        else:
            df_sells = df_year[df_year[type_col].str.contains('Sell', case=False, na=False)].copy()
            df_buys = df[df[type_col].str.contains('Buy', case=False, na=False)].copy()
        
        print(f"\nSell transactions this year: {len(df_sells)}")
        print(f"Buy transactions (all time): {len(df_buys)}")
        
        # Calculate P&L per stock
        pnl_by_stock = defaultdict(lambda: {
            'short_term_pnl': 0,
            'long_term_pnl': 0,
            'short_term_count': 0,
            'long_term_count': 0
        })
        
        print(f"\n{'='*80}")
        print("DETAILED P&L ANALYSIS")
        print(f"{'='*80}\n")
        
        for _, sell_row in df_sells.iterrows():
            symbol = sell_row[symbol_col]
            sell_date = sell_row[date_col]
            sell_qty = clean_currency(sell_row[qty_col])
            sell_price = clean_currency(sell_row[price_col])
            sell_proceeds = sell_qty * sell_price
            
            # Find matching buy transactions (FIFO)
            symbol_buys = df_buys[df_buys[symbol_col] == symbol].sort_values(date_col)
            
            remaining_qty = sell_qty
            total_cost = 0
            
            for _, buy_row in symbol_buys.iterrows():
                if remaining_qty <= 0:
                    break
                
                buy_date = buy_row[date_col]
                buy_qty = clean_currency(buy_row[qty_col])
                buy_price = clean_currency(buy_row[price_col])
                
                # Determine how many shares to match
                matched_qty = min(remaining_qty, buy_qty)
                matched_cost = matched_qty * buy_price
                total_cost += matched_cost
                
                # Calculate holding period
                holding_days = (sell_date - buy_date).days
                is_long_term = holding_days > 365
                
                matched_proceeds = matched_qty * sell_price
                pnl = matched_proceeds - matched_cost
                
                if is_long_term:
                    pnl_by_stock[symbol]['long_term_pnl'] += pnl
                    pnl_by_stock[symbol]['long_term_count'] += 1
                else:
                    pnl_by_stock[symbol]['short_term_pnl'] += pnl
                    pnl_by_stock[symbol]['short_term_count'] += 1
                
                print(f"{symbol}: Sell {matched_qty:.2f} @ ${sell_price:.2f} on {sell_date.date()}")
                print(f"  Matched with Buy {matched_qty:.2f} @ ${buy_price:.2f} on {buy_date.date()}")
                print(f"  Holding period: {holding_days} days ({'Long-term' if is_long_term else 'Short-term'})")
                print(f"  P&L: ${pnl:+.2f}\n")
                
                remaining_qty -= matched_qty
        
        # Print summary by stock
        print(f"\n{'='*80}")
        print("P&L SUMMARY BY STOCK")
        print(f"{'='*80}\n")
        print(f"{'Symbol':<10} {'Short-Term P&L':>15} {'Long-Term P&L':>15} {'Total P&L':>15}")
        print(f"{'-'*10} {'-'*15} {'-'*15} {'-'*15}")
        
        total_short_term = 0
        total_long_term = 0
        
        for symbol in sorted(pnl_by_stock.keys()):
            data = pnl_by_stock[symbol]
            st_pnl = data['short_term_pnl']
            lt_pnl = data['long_term_pnl']
            total_pnl = st_pnl + lt_pnl
            
            total_short_term += st_pnl
            total_long_term += lt_pnl
            
            print(f"{symbol:<10} ${st_pnl:>14.2f} ${lt_pnl:>14.2f} ${total_pnl:>14.2f}")
        
        print(f"{'-'*10} {'-'*15} {'-'*15} {'-'*15}")
        print(f"{'TOTAL':<10} ${total_short_term:>14.2f} ${total_long_term:>14.2f} ${total_short_term + total_long_term:>14.2f}")
        
        print(f"\n{'='*80}")
        print("TAX IMPLICATIONS")
        print(f"{'='*80}")
        print(f"Short-term gains (taxed as ordinary income): ${total_short_term:+.2f}")
        print(f"Long-term gains (preferential tax rate):     ${total_long_term:+.2f}")
        print(f"Total gains/losses:                           ${total_short_term + total_long_term:+.2f}")
        
        print(f"\n{'='*80}\n")
        
    except FileNotFoundError:
        print(f"Error: File not found: {ACTIVITY_CSV}")
    except Exception as e:
        print(f"Error calculating P&L: {e}")
        import traceback
        traceback.print_exc()
    finally:
        sys.stdout = sys.__stdout__
        log_file.close()

if __name__ == "__main__":
    calculate_pnl()

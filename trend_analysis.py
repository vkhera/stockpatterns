import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

def calculate_cagr(start_value, end_value, years):
    """Calculate Compound Annual Growth Rate (CAGR)"""
    if start_value <= 0 or end_value <= 0:
        return 0
    return (end_value / start_value) ** (1 / years) - 1

def analyze_trends_and_cagr(csv_file_path):
    """Analyze trends and calculate CAGR for all columns in the performance data"""
    
    # Read the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Print basic info about the dataset
    print(f"Dataset shape: {df.shape}")
    print(f"Date range: {df.iloc[0, 0]} to {df.iloc[-1, 0]}")
    print()
    
    # Convert first column to datetime and extract years
    start_date = datetime.strptime(df.iloc[0, 0], '%Y-%m-%d')
    end_date = datetime.strptime(df.iloc[-1, 0], '%Y-%m-%d')
    total_years = (end_date - start_date).days / 365.25
    
    print(f"Analysis period: {total_years:.2f} years")
    print("=" * 80)
    
    # Calculate CAGR for each numeric column
    cagr_results = []
    
    for col_idx in range(1, df.shape[1]):  # Skip the first column (date)
        col_name = f"Column_{col_idx}"
        
        # Get first and last non-zero values
        column_data = pd.to_numeric(df.iloc[:, col_idx], errors='coerce')
        
        # Find first and last non-zero values
        non_zero_mask = (column_data > 0) & (~column_data.isna())
        
        if non_zero_mask.sum() < 2:  # Need at least 2 non-zero values
            continue
            
        first_nonzero_idx = non_zero_mask.idxmax()
        last_nonzero_idx = non_zero_mask[::-1].idxmax()
        
        start_value = column_data.iloc[first_nonzero_idx]
        end_value = column_data.iloc[last_nonzero_idx]
        
        # Calculate years between first and last non-zero values
        start_date_col = datetime.strptime(df.iloc[first_nonzero_idx, 0], '%Y-%m-%d')
        end_date_col = datetime.strptime(df.iloc[last_nonzero_idx, 0], '%Y-%m-%d')
        years_col = (end_date_col - start_date_col).days / 365.25
        
        if years_col > 0:
            cagr = calculate_cagr(start_value, end_value, years_col)
            
            # Calculate some additional statistics
            max_value = column_data.max()
            min_value = column_data[column_data > 0].min() if (column_data > 0).any() else 0
            mean_value = column_data[column_data > 0].mean() if (column_data > 0).any() else 0
            std_value = column_data[column_data > 0].std() if (column_data > 0).any() else 0
            
            cagr_results.append({
                'column_index': col_idx,
                'column_name': col_name,
                'start_value': start_value,
                'end_value': end_value,
                'start_date': start_date_col.strftime('%Y-%m-%d'),
                'end_date': end_date_col.strftime('%Y-%m-%d'),
                'years': years_col,
                'cagr': cagr * 100,  # Convert to percentage
                'total_return': ((end_value / start_value) - 1) * 100,
                'max_value': max_value,
                'min_value': min_value,
                'mean_value': mean_value,
                'std_value': std_value,
                'coefficient_of_variation': (std_value / mean_value) if mean_value > 0 else 0
            })
    
    # Sort by CAGR in descending order
    cagr_results.sort(key=lambda x: x['cagr'], reverse=True)
    
    print("TOP 20 COLUMNS BY CAGR:")
    print("-" * 100)
    print(f"{'Rank':<4} {'Col#':<4} {'CAGR%':<8} {'Total Return%':<12} {'Start Value':<12} {'End Value':<12} {'Years':<6} {'Start Date':<12} {'End Date'}")
    print("-" * 100)
    
    for i, result in enumerate(cagr_results[:20]):
        print(f"{i+1:<4} {result['column_index']:<4} {result['cagr']:<8.2f} {result['total_return']:<12.2f} "
              f"{result['start_value']:<12.2f} {result['end_value']:<12.2f} {result['years']:<6.2f} "
              f"{result['start_date']:<12} {result['end_date']}")
    
    print("\n" + "=" * 80)
    print("DETAILED ANALYSIS OF TOP 5 PERFORMERS:")
    print("=" * 80)
    
    for i, result in enumerate(cagr_results[:5]):
        print(f"\nRANK {i+1}: Column {result['column_index']}")
        print(f"  CAGR: {result['cagr']:.2f}% per year")
        print(f"  Total Return: {result['total_return']:.2f}%")
        print(f"  Investment Period: {result['start_date']} to {result['end_date']} ({result['years']:.2f} years)")
        print(f"  Value Growth: {result['start_value']:.2f} → {result['end_value']:.2f}")
        print(f"  Statistics: Max={result['max_value']:.2f}, Min={result['min_value']:.2f}, Mean={result['mean_value']:.2f}")
        print(f"  Risk (CV): {result['coefficient_of_variation']:.2f}")
    
    # Identify trend patterns
    print("\n" + "=" * 80)
    print("TREND ANALYSIS:")
    print("=" * 80)
    
    # Categorize columns by performance
    high_performers = [r for r in cagr_results if r['cagr'] > 20]
    moderate_performers = [r for r in cagr_results if 10 <= r['cagr'] <= 20]
    low_performers = [r for r in cagr_results if 0 < r['cagr'] < 10]
    negative_performers = [r for r in cagr_results if r['cagr'] < 0]
    
    print(f"High Performers (>20% CAGR): {len(high_performers)} columns")
    print(f"Moderate Performers (10-20% CAGR): {len(moderate_performers)} columns")
    print(f"Low Performers (0-10% CAGR): {len(low_performers)} columns")
    print(f"Negative Performers (<0% CAGR): {len(negative_performers)} columns")
    
    print(f"\nOverall Portfolio Insights:")
    print(f"- Best performing column: #{cagr_results[0]['column_index']} with {cagr_results[0]['cagr']:.2f}% CAGR")
    print(f"- Average CAGR across all columns: {np.mean([r['cagr'] for r in cagr_results]):.2f}%")
    print(f"- Median CAGR: {np.median([r['cagr'] for r in cagr_results]):.2f}%")
    
    # Save detailed results to CSV
    results_df = pd.DataFrame(cagr_results)
    output_file = 'output/trend_analysis_results.csv'
    results_df.to_csv(output_file, index=False)
    print(f"\nDetailed results saved to: {output_file}")
    
    return cagr_results

if __name__ == "__main__":
    csv_file = "output/perf_trans.csv"
    results = analyze_trends_and_cagr(csv_file)

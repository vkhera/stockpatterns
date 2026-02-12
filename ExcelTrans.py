import pandas as pd
import os

# Path to the Excel file
excel_path = "Performance.xlsx"

try:
    # Read the first sheet
    df = pd.read_excel(excel_path, sheet_name=0)

    # Concatenate columns A and B for headers with empty check
    colA = df.iloc[:, 0]
    colB = df.iloc[:, 1]
    concat_headers = []
    for a, b in zip(colA, colB):
        a_str = '' if pd.isna(a) or str(a).strip() == '' else str(a)
        b_str = '' if pd.isna(b) or str(b).strip() == '' else str(b)
        if a_str:
            header = a_str + ('_' + b_str if b_str else '')
        else:
            header = b_str
        # Split header into words, remove duplicates, then join
        words = header.replace(',', ' ').replace('_', ' ').split()
        seen = set()
        unique_words = []
        for word in words:
            w = word.strip()
            if w and w.lower() not in seen:
                seen.add(w.lower())
                unique_words.append(w)
        header = '_'.join(unique_words)
        # Remove unwanted _-_, -_, _- patterns
        for pat in ['_-_', '-_', '_-']:
            header = header.replace(pat, '')
        # Make SQL friendly: remove spaces and commas
        header = header.replace(' ', '').replace(',', '')
        concat_headers.append(header)

    # Get columns to read: D (index 3) and every 9th column after D
    col_indices = [3] + [i for i in range(3+9, df.shape[1], 9)]
    selected_cols = df.iloc[:, col_indices]

    # Transpose selected columns, use concatenated A+B as header
    transposed = selected_cols.T
    transposed.columns = concat_headers

    # Remove columns that are completely empty (all values blank)
    transposed = transposed.loc[:, ~(transposed.isna() | (transposed == '')).all()]

    # Use first row as index (dates), strip time info if present
    def strip_time(val):
        if isinstance(val, str) and ' ' in val:
            return val.split(' ')[0]
        if isinstance(val, pd.Timestamp):
            return val.date()
        return val
    transposed.insert(0, 'Date', transposed.index.map(lambda i: strip_time(selected_cols.columns[i]) if isinstance(i, int) else strip_time(i)))
    # Save to CSV
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_path = os.path.join(output_dir, "perf_trans.csv")
    transposed.to_csv(output_path, index=False)
    print(f"Transformed data saved to {output_path}")
except Exception as e:
    print(f"Error processing Excel file: {e}")

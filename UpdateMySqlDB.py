# Store LLM responses from CSV to MySQL
def store_llm_responses_to_mysql(
    mysql_config=None,
    table_name='llm_response_record',
    csv_file=None
):
    """
    Store records from output/llm_response_record.csv to a MySQL table. Creates the table if it doesn't exist.
    mysql_config: dict with keys host, user, password, database (if None, uses load_db_config)
    table_name: name of the table to store records
    csv_file: path to the CSV file (default: output/llm_response_record.csv)
    """
    import mysql.connector
    import csv
    if csv_file is None:
        csv_file = os.path.join(os.path.dirname(__file__), 'output', 'llm_response_record.csv')
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        return
    db_cfg = mysql_config if mysql_config else load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    create_table_sql = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Symbol VARCHAR(32),
        Model VARCHAR(64),
        RequestTime VARCHAR(32),
        Prompt TEXT,
        ResponseTime VARCHAR(32),
        Analysis VARCHAR(32),
        Majority VARCHAR(32),
        ExecTimeSec VARCHAR(16)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    cursor.execute(create_table_sql)
    with open(csv_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]
    insert_sql = f'''
        INSERT INTO {table_name}
        (Symbol, Model, RequestTime, Prompt, ResponseTime, Analysis, Majority, ExecTimeSec)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    '''
    data = [
        (
            row['Symbol'],
            row['Model'],
            row['Request Time'],
            row['Prompt'],
            row['Response Time'],
            row['Analysis'],
            row['Majority'],
            row.get('Exec Time (s)', row.get('ExecTimeSec', ''))
        )
        for row in rows
    ]
    if data:
        cursor.executemany(insert_sql, data)
        conn.commit()
        print(f"Inserted {cursor.rowcount} records into {table_name}.")
    else:
        print("No records to insert.")
    cursor.close()
    conn.close()
import csv
import mysql.connector
import json
from cryptography.fernet import Fernet
import os

# Load DB config from config.json with encrypted password
def load_db_config(config_path='config.json'):
    with open(config_path, 'r') as f:
        config = json.load(f)
    key = config['key'].encode()
    fernet = Fernet(key)
    db_password = fernet.decrypt(config['mysql_password'].encode()).decode()
    db_config = {
        'host': config['mysql_host'],
        'user': config['mysql_user'],
        'password': db_password,
        'database': config['mysql_db']
    }
    return db_config

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'output')

def insert_robinhood_holdings(
    csv_file=os.path.join(OUTPUT_DIR, 'holdings_report.csv'),
    table_name='robinhood_holdings',
    db_params=None
):
    """
    Create the robinhood_holdings table if not exists and insert records from the given CSV file.
    Args:
        csv_file (str): Path to the Robinhood holdings CSV file.
        table_name (str): Name of the MySQL table to insert into.
        db_params (dict): Optional DB connection params (default: use db_config).
    """
    schema = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Symbol VARCHAR(32),
        Quantity VARCHAR(32),
        Price VARCHAR(32),
        Equity VARCHAR(32),
        Percent_Change VARCHAR(32),
        Type VARCHAR(32),
        Beta VARCHAR(16),
        Trend VARCHAR(32)
    )
    """
    insert_stmt = f"""
    INSERT INTO {table_name} (
        Symbol, Quantity, Price, Equity, Percent_Change, Type, Beta, Trend
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    db_cfg = db_params if db_params else load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    cursor.execute(schema)
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 8:
                row += [''] * (8 - len(row))
            cursor.execute(insert_stmt, row)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Robinhood records from {csv_file} inserted into {table_name} successfully.")

def insert_brokerage_holdings(
    csv_file=os.path.join(OUTPUT_DIR, 'holdings_cleaned.csv'),
    table_name='holdings',
    db_params=None
):
    """
    Create the holdings table if not exists and insert records from the given CSV file.
    Args:
        csv_file (str): Path to the brokerage holdings CSV file.
        table_name (str): Name of the MySQL table to insert into.
        db_params (dict): Optional DB connection params (default: use db_config).
    """
    schema = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Ticker VARCHAR(32),
        Symbol_Description VARCHAR(128),
        Empty1 VARCHAR(8),
        Quantity VARCHAR(32),
        Price VARCHAR(32),
        Days_Change VARCHAR(32),
        Value VARCHAR(32),
        Days_Value_Change VARCHAR(32),
        Unrealized_Gain_Loss VARCHAR(64),
        Last_Updated VARCHAR(32),
        Empty2 VARCHAR(8),
        Beta VARCHAR(16),
        Trend VARCHAR(32),
        Exported_On VARCHAR(32)
    )
    """
    insert_stmt = f"""
    INSERT INTO {table_name} (
        Ticker, Symbol_Description, Empty1, Quantity, Price, Days_Change, Value,
        Days_Value_Change, Unrealized_Gain_Loss, Last_Updated, Empty2, Beta, Trend, Exported_On
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    db_cfg = db_params if db_params else load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    cursor.execute(schema)
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 14:
                row += [''] * (14 - len(row))
            cursor.execute(insert_stmt, row)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Brokerage records from {csv_file} inserted into {table_name} successfully.")

def delete_holdings_table(table_name='holdings', db_params=None):
    """
    Delete the specified holdings table from the MySQL database if it exists.
    Args:
        table_name (str): Name of the table to delete.
        db_params (dict): Optional DB connection params (default: use db_config).
    """
    db_cfg = db_params if db_params else load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Table '{table_name}' deleted (if it existed).")

# Optionally, keep the main() for CLI usage, but now only for both tables
def main():
#    delete_holdings_table('holdings')
    # Insert holdings_cleaned.csv into 'holdings' table
    table_name = 'holdings'
    csv_file = 'holdings_cleaned.csv'
    table_schema = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Ticker VARCHAR(32),
        Symbol_Description VARCHAR(128),
        Empty1 VARCHAR(8),
        Quantity VARCHAR(32),
        Price VARCHAR(32),
        Days_Change VARCHAR(32),
        Value VARCHAR(32),
        Days_Value_Change VARCHAR(32),
        Unrealized_Gain_Loss VARCHAR(64),
        Last_Updated VARCHAR(32),
        Empty2 VARCHAR(8),
        Beta VARCHAR(16),
        Trend VARCHAR(32),
        Exported_On VARCHAR(32)
    )
    """
    insert_stmt = f"""
    INSERT INTO {table_name} (
        Ticker, Symbol_Description, Empty1, Quantity, Price, Days_Change, Value,
        Days_Value_Change, Unrealized_Gain_Loss, Last_Updated, Empty2, Beta, Trend, Exported_On
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    db_config = load_db_config()
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute(table_schema)
    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        for row in reader:
            if len(row) < 14:
                row += [''] * (14 - len(row))
            cursor.execute(insert_stmt, row)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Records from {csv_file} inserted into {table_name} successfully.")

if __name__ == "__main__":
    main()
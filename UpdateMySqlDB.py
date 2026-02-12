def store_performance_csv_to_db():
    """
    Reads output/perf_trans.csv and stores its data in the Performance table in MySQL.
    Drops and recreates the table, cleans column names, and ensures uniqueness and compatibility.
    """
    import pandas as pd
    import mysql.connector
    import os

    # Read CSV
    csv_path = os.path.join(os.path.dirname(__file__), 'output', 'perf_trans.csv')
    df = pd.read_csv(csv_path)

    # Connect to MySQL using config.json
    db_cfg = load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()

    # Clean DataFrame columns: replace NaN/empty with unique placeholder
    cleaned_columns = []
    for i, col in enumerate(df.columns):
        col_str = str(col).strip()
        if col_str.lower() == 'nan' or col_str == '' or pd.isna(col):
            col_str = f'col_{i}'
        cleaned_columns.append(col_str)
    df.columns = cleaned_columns

    # Build unique column names for MySQL (trim to 60 chars and add index for uniqueness)
    unique_columns = [f"{col[:60]}_{i}" for i, col in enumerate(df.columns)]
    col_defs = ', '.join([f'`{col}` VARCHAR(128)' for col in unique_columns])
    # Drop and recreate table to ensure no old columns remain
    cursor.execute("DROP TABLE IF EXISTS Performance")
    create_table_sql = f"CREATE TABLE Performance ({col_defs})"
    cursor.execute(create_table_sql)

    # Insert data
    for _, row in df.iterrows():
        filtered_row = [row[col] for col in df.columns]
        placeholders = ', '.join(['%s'] * len(unique_columns))
        insert_sql = f"INSERT INTO Performance ({', '.join([f'`{col}`' for col in unique_columns])}) VALUES ({placeholders})"
        cursor.execute(insert_sql, tuple(filtered_row))

    conn.commit()
    cursor.close()
    conn.close()
    print('Performance data stored in MySQL database.')
import csv
import json
from cryptography.fernet import Fernet

import pandas as pd
import mysql.connector
import os

def store_llm_responses_to_mysql(mysql_config=None, table_name='llm_response_record', csv_file=None):
    """
    Store records from output/llm_response_record.csv to a MySQL table. Creates the table if it doesn't exist.
    Only inserts new records based on unique composite key (Symbol, Model, RequestTime, Prompt).
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
    
    # Create table if it doesn't exist (without unique constraint initially)
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
    conn.commit()
    cursor.close()
    conn.close()
    
    # Run migration to add unique constraint (safe to run multiple times)
    migrate_llm_response_table(mysql_config, table_name)
    
    # Reconnect for insert operations
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    
    # Read CSV file and prepare data
    with open(csv_file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = [row for row in reader]
    
    # Insert data using INSERT IGNORE to skip duplicates
    insert_sql = f'''
        INSERT IGNORE INTO {table_name}
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
        print(f"Inserted {cursor.rowcount} new records into {table_name} (duplicates skipped).")
    else:
        print("No records to insert.")
    
    cursor.close()
    conn.close()

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

def kill_other_db_connections(mysql_config=None, exclude_current=True):
    """
    Kill all other MySQL connections to the current database to avoid lock conflicts.
    """
    import mysql.connector
    db_cfg = mysql_config if mysql_config else load_db_config()
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    
    current_connection_id = conn.connection_id
    database = db_cfg['database']
    
    # Get all connection IDs for the current database
    cursor.execute(f"""
        SELECT id FROM information_schema.processlist 
        WHERE db = '{database}' 
        AND id != {current_connection_id if exclude_current else 0}
        AND command != 'Sleep'
    """)
    
    connection_ids = [row[0] for row in cursor.fetchall()]
    
    if connection_ids:
        print(f"Found {len(connection_ids)} other active connections. Killing them...")
        for conn_id in connection_ids:
            try:
                cursor.execute(f"KILL {conn_id}")
                print(f"Killed connection ID: {conn_id}")
            except mysql.connector.Error as e:
                print(f"Could not kill connection {conn_id}: {e}")
        conn.commit()
    else:
        print("No other active connections found.")
    
    cursor.close()
    conn.close()

def migrate_llm_response_table(mysql_config=None, table_name='llm_response_record'):
    """
    One-time migration: Drop and recreate table with UNIQUE constraint.
    Safe to run multiple times (checks if constraint already exists).
    """
    import mysql.connector
    db_cfg = mysql_config if mysql_config else load_db_config()
    
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if not cursor.fetchone():
        print(f"Table {table_name} does not exist yet. Skipping migration.")
        cursor.close()
        conn.close()
        return
    
    # Check if unique constraint already exists
    cursor.execute(f"""
        SELECT COUNT(*) FROM information_schema.statistics 
        WHERE table_schema = DATABASE() 
        AND table_name = '{table_name}' 
        AND index_name = 'unique_record'
    """)
    constraint_exists = cursor.fetchone()[0] > 0
    
    if constraint_exists:
        print(f"UNIQUE constraint already exists on {table_name}. Skipping migration.")
        cursor.close()
        conn.close()
        return
    
    # Get total record count
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    total_records = cursor.fetchone()[0]
    print(f"Total records in {table_name}: {total_records}")
    
    cursor.close()
    conn.close()
    
    # Kill other connections before dropping table
    print("Killing other database connections to avoid locks...")
    kill_other_db_connections(mysql_config)
    
    # Reconnect and drop table
    conn = mysql.connector.connect(**db_cfg)
    cursor = conn.cursor()
    
    print(f"Dropping and recreating {table_name} with UNIQUE constraint...")
    
    # Disable foreign key checks for faster drop
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    conn.commit()
    
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()
    
    # Re-enable foreign key checks
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    
    # Recreate table with UNIQUE constraint
    create_table_sql = f'''
    CREATE TABLE {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Symbol VARCHAR(32),
        Model VARCHAR(64),
        RequestTime VARCHAR(32),
        Prompt TEXT,
        ResponseTime VARCHAR(32),
        Analysis VARCHAR(32),
        Majority VARCHAR(32),
        ExecTimeSec VARCHAR(16),
        UNIQUE KEY unique_record (Symbol, Model, RequestTime)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    cursor.execute(create_table_sql)
    conn.commit()
    print(f"Table {table_name} recreated with UNIQUE constraint. All {total_records} old records removed.")
    
    cursor.close()
    conn.close()

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
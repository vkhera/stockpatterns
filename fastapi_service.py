# GET endpoint for llm_response_record table
@app.get("/llm_responses/")
def get_llm_responses(
    model: Optional[str] = Query(None, description="Filter by model name"),
    majority: Optional[str] = Query(None, description="Filter by majority sentiment (positive/negative/neutral)"),
    limit: int = Query(100, description="Max number of records to return")
):
    db_config = load_db_config()
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM llm_response_record"
    params = []
    filters = []
    if model:
        filters.append("Model = %s")
        params.append(model)
    if majority:
        filters.append("Majority = %s")
        params.append(majority)
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " LIMIT %s"
    params.append(limit)
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"data": results}
from fastapi import FastAPI, Query
from typing import Optional
import mysql.connector
import json
from cryptography.fernet import Fernet

app = FastAPI()

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

@app.get("/holdings/")
def get_holdings(
    ticker: Optional[str] = Query(None, description="Filter by ticker symbol"),
    limit: int = Query(100, description="Max number of records to return")
):
    db_config = load_db_config()
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM holdings"
    params = []
    if ticker:
        query += " WHERE Ticker = %s"
        params.append(ticker)
    query += " LIMIT %s"
    params.append(limit)
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"data": results}

@app.get("/robinhood_holdings/")
def get_robinhood_holdings(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    limit: int = Query(100, description="Max number of records to return")
):
    db_config = load_db_config()
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM robinhood_holdings"
    params = []
    if symbol:
        query += " WHERE Symbol = %s"
        params.append(symbol)
    query += " LIMIT %s"
    params.append(limit)
    cursor.execute(query, params)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return {"data": results}

# To run: uvicorn fastapi_service:app --reload

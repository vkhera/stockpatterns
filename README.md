# Stock Portfolio Analysis & Management System

A comprehensive Python-based system for analyzing stock portfolios, integrating with multiple brokerage APIs, performing sentiment analysis using LLM models, and providing data visualization and storage capabilities.

## 🚀 Quick Start

1. **Setup Environment:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Credentials:**
   ```bash
   python encrypt_password.py
   ```

3. **Run Main Analysis:**
   ```bash
   python main.py
   ```

## 📁 Project Structure

### Core Files

#### `main.py` - Main Orchestrator
**Purpose:** Central entry point that coordinates all portfolio analysis workflows.

**Key Features:**
- Downloads Robinhood holdings data
- Calculates portfolio risk metrics
- Performs news sentiment analysis using multiple LLM models
- Stores data in MySQL database
- Generates accuracy statistics

**Usage:**
```bash
python main.py
```

**What it does:**
1. Initializes RobinhoodPortfolio with encrypted credentials
2. Downloads current holdings and calculates risk
3. Fetches news and analyzes sentiment using Ollama LLM models
4. Stores LLM responses in MySQL
5. Calculates and exports model accuracy statistics

---

#### `robinhood.py` - Robinhood API Integration
**Purpose:** Core class for interacting with Robinhood API and performing portfolio analysis.

**Key Features:**
- Secure login with encrypted credentials
- Holdings download with Beta and trend analysis
- Portfolio risk calculation (Beta, standard deviation)
- News sentiment analysis using multiple LLM models (Ollama)
- Majority voting system for sentiment analysis

**Key Methods:**
- `download_holdings()` - Downloads current positions with trend analysis
- `calculate_portfolio_risk()` - Calculates portfolio Beta and volatility
- `fetch_and_analyze_news()` - Gets news and analyzes sentiment using 3 LLM models
- `analyze_sentiment_with_ollama()` - Calls multiple models and returns majority sentiment

**Usage:**
```python
from robinhood import RobinhoodPortfolio
robin = RobinhoodPortfolio('config.json')
robin.download_holdings()
robin.fetch_and_analyze_news()
```

---

#### `UpdateMySqlDB.py` - Database Operations
**Purpose:** Handles all MySQL database operations for storing portfolio and analysis data.

**Key Features:**
- Encrypted database connection management
- Robinhood holdings storage
- Brokerage data import from CSV
- LLM response logging with timing metrics
- Performance data storage

**Key Functions:**
- `insert_robinhood_holdings()` - Stores Robinhood portfolio data
- `insert_brokerage_holdings()` - Imports external brokerage CSV data
- `store_llm_responses_to_mysql()` - Logs LLM sentiment analysis results
- `store_performance_csv_to_db()` - Imports performance tracking data

**Usage:**
```python
from UpdateMySqlDB import insert_robinhood_holdings, store_llm_responses_to_mysql
insert_robinhood_holdings()
store_llm_responses_to_mysql()
```

---

#### `fastapi_service.py` - REST API Service
**Purpose:** Provides REST API endpoints for accessing stored portfolio and analysis data.

**Features:**
- GET endpoint for LLM response data with filtering
- Encrypted database connection
- Query parameters for model and sentiment filtering
- Configurable result limits

**API Endpoints:**
- `GET /llm_responses/` - Retrieve LLM sentiment analysis results
  - Query params: `model`, `majority`, `limit`

**Usage:**
```bash
# Start the API server
uvicorn fastapi_service:app --reload

# Example API calls
curl "http://localhost:8000/llm_responses/?model=llama3.2:latest&limit=50"
curl "http://localhost:8000/llm_responses/?majority=positive"
```

---

#### `Accuracy.py` - Model Performance Analysis
**Purpose:** Calculates accuracy metrics and execution timing for LLM sentiment analysis models.

**Features:**
- Compares individual model predictions against majority vote
- Calculates accuracy percentages per model
- Tracks execution time statistics
- Exports results to CSV

**Key Function:**
- `calculate_model_accuracy_and_timing()` - Analyzes LLM model performance

**Output:** Creates `output/Accuracy.csv` with model performance metrics

**Usage:**
```python
from Accuracy import calculate_model_accuracy_and_timing
calculate_model_accuracy_and_timing()
```

---

#### `trend_analysis.py` - Financial Trend Analysis
**Purpose:** Analyzes historical performance data to calculate CAGR and identify investment trends.

**Features:**
- Calculates Compound Annual Growth Rate (CAGR) for all investment columns
- Ranks investments by performance
- Identifies trend patterns and risk metrics
- Generates comprehensive performance reports

**Key Function:**
- `analyze_trends_and_cagr()` - Analyzes performance data and calculates CAGR

**Usage:**
```bash
python trend_analysis.py
```

**Output:** 
- Console report with top performers and trend analysis
- `output/trend_analysis_results.csv` with detailed CAGR calculations

---

### Utility & Configuration Files

#### `encrypt_password.py` - Secure Credential Setup
**Purpose:** Interactive script for securely encrypting and storing API credentials.

**Features:**
- Generates encryption keys
- Encrypts Robinhood and MySQL passwords
- Creates `config.json` with encrypted credentials

**Usage:**
```bash
python encrypt_password.py
```

**Prompts for:**
- Robinhood username and password
- MySQL connection details
- Generates encrypted config.json file

---

#### `ExcelTrans.py` - Excel Data Transformation
**Purpose:** Converts Excel performance data to CSV format with cleaned headers.

**Features:**
- Reads Excel files and processes complex headers
- Combines multiple header rows
- Removes duplicates and creates SQL-friendly column names
- Extracts specific column patterns (every 9th column)

**Usage:**
```bash
python ExcelTrans.py
```

**Input:** `Performance.xlsx`
**Output:** `output/perf_trans.csv`

---

### Brokerage Integration Files

#### `ICICIDirectBreezeOne.py` - ICICI Direct API
**Purpose:** Integrates with ICICI Direct brokerage API using Breeze Connect.

**Features:**
- ICICI Direct API authentication
- Portfolio and demat holdings retrieval
- Scheduled data fetching capabilities

**Note:** Contains API keys and session tokens - should be configured with your credentials.

#### `ICICIDirectBreezeTwo.py` - Extended ICICI Integration
**Purpose:** Additional ICICI Direct functionality and alternative implementation.

#### `fidelity_download.py` - Fidelity Integration
**Purpose:** Handles Fidelity brokerage data download and processing.

#### `ml_export_to_holdings.py` - ML Data Export
**Purpose:** Exports machine learning analysis results to holdings format.

#### `otp_reader.py` - OTP Management
**Purpose:** Handles One-Time Password reading for secure authentication.

---

## 📊 Data Flow

1. **Data Collection:**
   - Robinhood API → Holdings data
   - External brokerages → CSV imports
   - News APIs → Sentiment analysis data

2. **Analysis Pipeline:**
   - Risk calculation (Beta, volatility)
   - Trend analysis (30-day slopes)
   - LLM sentiment analysis (3-model majority voting)
   - CAGR calculations for historical performance

3. **Storage & Access:**
   - MySQL database for structured data
   - CSV exports for analysis
   - REST API for data access

## 🔧 Configuration

### Required Files:
- `config.json` - Encrypted credentials (created by `encrypt_password.py`)
- `holdings_cleaned.csv` - External brokerage data (optional)
- `Performance.xlsx` - Historical performance data (optional)

### Environment Variables:
- Python 3.8+ required
- MySQL database connection
- Ollama LLM service running locally

## 📈 Output Files

All output files are stored in the `output/` directory:

- `holdings_report.csv` - Current Robinhood holdings with analysis
- `llm_response_record.csv` - LLM sentiment analysis logs
- `Accuracy.csv` - Model performance metrics
- `trend_analysis_results.csv` - CAGR and trend analysis
- `perf_trans.csv` - Processed performance data

## 🚨 Security Notes

- All passwords are encrypted using Fernet encryption
- API keys should be kept secure and not committed to version control
- Database credentials are encrypted in config.json
- Use environment variables for production deployments

## 📋 Dependencies

See `requirements.txt` for full dependency list. Key libraries:
- `robin-stocks` - Robinhood API
- `pandas`, `numpy` - Data analysis
- `fastapi`, `uvicorn` - REST API
- `mysql-connector-python` - Database
- `yfinance` - Market data
- `cryptography` - Encryption

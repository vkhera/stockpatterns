from robinhood import RobinhoodPortfolio
from UpdateMySqlDB import insert_robinhood_holdings, main as insert_brokerage_holdings, store_llm_responses_to_mysql

robin = RobinhoodPortfolio('config.json')
#robin.download_holdings()
#robin.download_holdings_all_positions()
#robin.download_portfolio_profile()
#robin.download_open_stock_positions()
#robin.calculate_portfolio_risk()
print("Portfolio risk calculated.")
#robin.fetch_and_analyze_news()
print("News fetched and analyzed.")
#robin.analyze_trends()
print("Trends analyzed.")

# Insert Robinhood holdings into MySQL
def insert_robinhood_to_mysql():
    insert_robinhood_holdings()

# Insert brokerage (holdings_cleaned.csv) into MySQL
def insert_brokerage_to_mysql():
    insert_brokerage_holdings()
def store_llm_responses():
    store_llm_responses_to_mysql()

# Example usage:
#insert_robinhood_to_mysql()
#insert_brokerage_to_mysql()
store_llm_responses()
print("LLM responses stored in MySQL.")

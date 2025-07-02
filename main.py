from robinhood import RobinhoodPortfolio
from UpdateMySqlDB import insert_robinhood_holdings, main as insert_brokerage_holdings

robin = RobinhoodPortfolio('config.json')
robin.download_holdings()
#robin.download_holdings_all_positions()
#robin.download_portfolio_profile()
robin.download_open_stock_positions()
#robin.calculate_portfolio_risk()
#robin.fetch_and_analyze_news()
#robin.analyze_trends()

# Insert Robinhood holdings into MySQL
def insert_robinhood_to_mysql():
    insert_robinhood_holdings()

# Insert brokerage (holdings_cleaned.csv) into MySQL
def insert_brokerage_to_mysql():
    insert_brokerage_holdings()

# Example usage:
#insert_robinhood_to_mysql()
#insert_brokerage_to_mysql()
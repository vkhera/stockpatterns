from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd
import yfinance as yf
import numpy as np
from multiprocessing import Pool
import schedule
import time
from datetime import datetime
import robin_stocks.robinhood as rs
import sys
from cryptography.fernet import Fernet
import configparser

# Constants
Brokerage_account_number = ""
TEST_MODE = True
USER = ""
PWD = ""
# Get the current timestamp
timestamp = int(time.time())

# Assigning the correct code to run when we use login()
def login():
    rs.authentication.login(
        username = USER,
        password = PWD,
        expiresIn = 86400,
        by_sms = True
    )

# Assigning the correct code to run when we use logout()
def logout():
    rs.authentication.logout()


# Format the timestamp as a string
def analyze_stock_trend(symbol):
    # Retrieve historical data
    ticker = yf.Ticker(symbol)
    historical_data = ticker.history(period='1d', interval='1m')

    # Feature Engineering
    features = ['Close', 'Volume']
    historical_data['Target'] = historical_data['Close'].shift(-5) > historical_data['Close']

    # Preprocess Data
    X = historical_data[features][:-5]  # Excluding last 5 minutes for which we don't have a target
    y = historical_data['Target'][:-5]

    # Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

    # Normalize Features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train the KNN Model
    knn = KNeighborsClassifier(n_neighbors=5)
    knn.fit(X_train_scaled, y_train)

    # Function to analyze trend
    def analyze_trend(last_minutes):
        last_minutes_data = historical_data[-last_minutes:-5]
        last_minutes_scaled = scaler.transform(last_minutes_data[features])
        predictions = knn.predict(last_minutes_scaled)
        return np.sum(predictions), len(predictions) - np.sum(predictions)

    # Analyzing the last 5 and 10 minutes
    uptrend_5, downtrend_5 = analyze_trend(10)
    uptrend_10, downtrend_10 = analyze_trend(15)

    # Determine the trend based on majority
    trend_5 = "Uptrend" if uptrend_5 > downtrend_5 else "Downtrend"
    trend_10 = "Uptrend" if uptrend_10 > downtrend_10 else "Downtrend"

    # Overall Trend
    overall_trend = trend_5 if trend_5 == trend_10 else "Mixed"

    return trend_5, trend_10, overall_trend

def cancelallorders():
    print("Inside cancelallorders")
    orders = rs.orders.get_all_open_stock_orders(info=None)
    # Count the number of open orders
    num_orders = len(orders)
    # Print the number of open orders
    print(f"Number of open orders: {num_orders}")
    # Check if num_orders is greater than 10
    if num_orders > 70:
        print("There are more than 15 open orders. Stopping.")
        if TEST_MODE == False:
            rs.orders.cancel_all_stock_orders()
        return

def get_open_stock_orders(amount_dollar,target_price,symbol):
    # Load account profile as a string
    profile_response = rs.profiles.load_account_profile(account_number=Brokerage_account_number, info='buying_power')

    #print(rs.crypto.get_crypto_quote('ETH', info=None))

    # Convert the string response to an integer
    try:
        stock_buying_power = float(profile_response)
    except ValueError:
        print("Unable to convert buying power to an integer.")
        stock_buying_power = 0


    # Check if crypto_buying_power is less than 100
    if stock_buying_power < 1000:
        print("Buying Power is less than 1000. Stopping.")
        return

    # Get open crypto orders
    orders = rs.orders.get_all_open_stock_orders(info=None)

    # Count the number of open orders
    num_orders = len(orders)

    # Print the number of open orders
    print(f"Number of open orders: {num_orders}")

    # Check if num_orders is greater than 10
    if num_orders > 70:
        print("There were more than 9 open buy orders. Cancelling all these orders...")
        if TEST_MODE == False:
            rs.orders.cancel_all_stock_orders()
        return
    print(f"Trying : order_buy_stock_limit_by_price-", {symbol})
    # Place a buy order for ETH with the specified parameters
    if TEST_MODE == False:
        rs.orders.order_buy_stock_limit_by_price(symbol, amount_dollar, target_price, timeInForce='gtc')

################# close order function

def get_close_stock_orders(amount_dollar,target_price,symbol):
    # Load account profile as a string
    profile_response = rs.profiles.load_account_profile(account_number=Brokerage_account_number, info='buying_power')

    #print(rs.crypto.get_crypto_quote('ETH', info=None))

    # Convert the string response to an integer
    try:
        stock_buying_power = float(profile_response)
    except ValueError:
        print("Unable to convert crypto buying power to an integer.")
        stock_buying_power = 0
    #
    # # Print crypto buying power
    # print("Crypto Buying Power:", crypto_buying_power)

    # Check if crypto_buying_power is less than 100
    if stock_buying_power < 1000:
        print("Buying Power is less than 1000. Stopping.")
        # Log out of Robinhood
        return

    # Get open crypto orders
    orders = rs.orders.get_all_open_stock_orders(info=None)

    # Count the number of open orders
    num_orders = len(orders)
  #  print(rs.account.load_phoenix_account(info='crypto'))

    # Print the number of open orders
    print(f"Number of open orders: {num_orders}")

    # Check if num_orders is greater than 10
    if num_orders > 70:
        print("There were more than 9 open sell orders. Cancelling all these orders...")
        if TEST_MODE == False:
            rs.orders.cancel_all_stock_orders()
        return
    print(f"Inside : order_sell_stock_limit_by_price-", {symbol})
    # Place a buy order for ETH with the specified parameters
    if TEST_MODE == False:
        rs.orders.order_sell_stock_limit_by_price(symbol, amount_dollar, target_price, timeInForce='gtc')


def getBTCRSI_1d_1m(symbol):
    # Fetch historical data from Yahoo Finance with a 1-minute interval for the last 1 day
    bitcoin_data = yf.Ticker(symbol)
    historical_data = bitcoin_data.history(period="2d", interval="1m")
    # Calculate minute-by-minute price changes
    historical_data["PriceChange"] = historical_data["Close"].diff()
    # Calculate the average gain and average loss over a 14-period
    average_gain = historical_data["PriceChange"].apply(lambda x: x if x > 0 else 0).rolling(window=14).mean()
    average_loss = -historical_data["PriceChange"].apply(lambda x: x if x < 0 else 0).rolling(window=14).mean()
    # Calculate the Relative Strength (RS)
    relative_strength = average_gain / average_loss
    # Calculate the RSI
    rsi = 100 - (100 / (1 + relative_strength))
    # Add the RSI values to the historical data
    historical_data["RSI"] = rsi
    # Get the last RSI value
    last_rsi_value = historical_data["RSI"].iloc[-1]
    return last_rsi_value

# Function to process a single symbol
def process_symbol(symbol):
    try:
        # Fetch intraday data
        ticker = yf.Ticker(symbol)
        historical_data = ticker.history(period='1d', interval='1m')

        historical_data['VWAP'] = (historical_data['Volume'] * historical_data['Close']).cumsum() / historical_data[
            'Volume'].cumsum()

        latest_vwap = historical_data['VWAP'].iloc[-1]

        # Calculate standard deviation with Bessel's correction
        vwap_std = np.std(historical_data['VWAP'])

        # Get the Bitcoin price data with a 1-minute timeframe
        bitcoin_data_current = yf.Ticker(symbol).history(period="1m")

        if bitcoin_data_current is not None and not bitcoin_data_current.empty:
            # Get the current Bitcoin close price (regular trading hours)
            close_price = bitcoin_data_current["Close"].iloc[-1]

            # Calculate the percentage difference between latest VWAP and current close price
            percentage_difference = ((close_price - latest_vwap) / latest_vwap) * 100

            # Print the close price and percentage difference
            print("The current close price is ${:.4f} USD".format(close_price))
            print("Percentage Difference: {:.2f}%".format(percentage_difference))
            print("VWAP is: {:.4f}".format(latest_vwap))
            # Define thresholds for notifications
            positive_thresholds = [0.55,1, 2, 3]
            negative_thresholds = [-0.55,-1, -2, -3]

            # Define buy functions
            def dummy_function_A():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                print(f"{overall_trend} is overall trend")
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                if float(last_rsi) < 28.00:
                    if overall_trend == "Downtrend":
                        get_open_stock_orders(float(0.25), round(float(close_price), 2),symbol)
                        print("Executing Dummy Function A")
                    elif overall_trend == "Mixed":
                        get_open_stock_orders(float(0.20), round(float(close_price), 2),symbol)
                        print("Executing Dummy Function A")

            def dummy_function_B():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                print(f"{overall_trend} is overall trend")
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                if float(last_rsi) < 28.00:
                    if overall_trend == "Downtrend":
                        get_open_stock_orders(float(0.45), round(float(close_price), 2),symbol)
                        print("Executing Dummy Function B")
                    elif overall_trend == "Mixed":
                        get_open_stock_orders(float(0.40), round(float(close_price), 2),symbol)
                        print("Executing Dummy Function B")


            def dummy_function_C():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                if float(last_rsi) < 28.00:
                    if trend_10_min == "Downtrend" and overall_trend == "Mixed":
                        get_open_stock_orders(float(0.80), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function C")
                    elif overall_trend == "Mixed":
                        get_open_stock_orders(float(0.70), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function C")

            def dummy_function_D():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                last_rsi = round(getBTCRSI_1d_1m("ETH-USD"), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) < 28.00:
                    if trend_10_min == "Downtrend" and overall_trend == "Mixed":
                        get_open_stock_orders(float(2.95), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function D")
                    elif overall_trend == "Mixed":
                        get_open_stock_orders(float(2.85), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function D")


            def dummy_function_E():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) < 28.00:
                    if trend_10_min == "Downtrend" and overall_trend == "Mixed":
                        get_open_stock_orders(float(9.05), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function E")
                    elif overall_trend == "Mixed":
                        get_open_stock_orders(float(10.55), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function E")

            ####################### Sell functions

            def dummy_function_X():
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) > 69.00:
                    print("Executing Dummy Function X")
                    get_close_stock_orders(float(0.35), round(float(close_price), 2), symbol)

            def dummy_function_Y():
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) > 69.00:
                    get_close_stock_orders(float(0.55), round(float(close_price), 2), symbol)
                    print("Executing Dummy Function Y")

            def dummy_function_Z():
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) > 69.00:
                    get_close_stock_orders(float(0.75), round(float(close_price), 2), symbol)
                    print("Executing Dummy Function Z")

            def dummy_function_W():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) > 69.00:
                    if overall_trend == "Uptrend":
                        get_close_stock_orders(float(5.95), round(float(close_price), 2), symbol)
                        get_close_stock_orders(float(11.05), round(float(close_price) * 1.01, 2), symbol)
                        get_close_stock_orders(float(24.05), round(float(close_price) * 1.02, 2), symbol)
                        print("Executing Dummy Function W")
                    elif overall_trend == "Mixed":
                        get_close_stock_orders(float(4.95), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function W")

            def dummy_function_V():
                trend_5_min, trend_10_min, overall_trend = analyze_stock_trend(symbol)
                last_rsi = round(getBTCRSI_1d_1m(symbol), 2)
                print(f"RSI is: {last_rsi}")
                # Check if the RSI is below 50.00
                if float(last_rsi) > 69.00:
                    if overall_trend == "Uptrend":
                        get_close_stock_orders(float(15.95), round(float(close_price), 2), symbol)
                        get_close_stock_orders(float(30.95), round(float(close_price) * 1.01, 2), symbol)
                        print("Executing Dummy Function V")
                    elif overall_trend == "Mixed":
                        get_close_stock_orders(float(10.95), round(float(close_price), 2), symbol)
                        print("Executing Dummy Function V")

            ######################################


            if percentage_difference >= -0.45:
                dummy_function_A()
            elif -0.46 > percentage_difference >= -0.75:
                dummy_function_B()
                #my_channel.push_note("ETH buy order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif -0.76 > percentage_difference >= -1.5:
                dummy_function_C()
                #my_channel.push_note("ETH buy order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif -1.6 > percentage_difference >= -3.5:
                dummy_function_D()
                #my_channel.push_note("ETH buy order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif -3.6 > percentage_difference >= -8:
                dummy_function_E()
                #my_channel.push_note("ETH buy order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif percentage_difference < -8.1:
                print("Too much of a drop - no buy orders placed")
                #my_channel.push_note("Excess ETH price drop", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
                sys.exit()


            #
            if percentage_difference <= 0.35:
                dummy_function_X()
            elif 0.36 < percentage_difference <= 0.85:
                dummy_function_Y()
                #my_channel.push_note("ETH sell order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif 0.86 < percentage_difference <= 2.95:
                dummy_function_Z()
                #my_channel.push_note("ETH sell order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif 2.96 < percentage_difference <= 6.5:
                dummy_function_W()
                #my_channel.push_note("ETH sell order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")
            elif percentage_difference > 6.6:
                dummy_function_V()
                #my_channel.push_note("ETH sell order", f"{symbol} Price {round(float(format(close_price)), 2)} USD")

            # Send notifications based on thresholds
            for threshold in positive_thresholds:
                if percentage_difference >= threshold:
                    # Send notification for positive percentage difference above threshold
                    message = f"Price above VWAP by: {percentage_difference:.2f}% (Threshold: {threshold}%)"
                    message += f"\nDate and Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    #my_channel.push_note(f"{symbol} Price {round(float(format(close_price)), 2)} USD", message)

            for threshold in negative_thresholds:
                if percentage_difference <= threshold:
                    # Send notification for negative percentage difference below threshold
                    message = f"Price below VWAP by: {percentage_difference:.2f}% (Threshold: {threshold}%)"
                    message += f"\nDate and Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    #my_channel.push_note(f"{symbol} Price {round(float(format(close_price)), 2)} USD", message)
                    # Check the percentage_difference against thresholds


        else:
            print(f"Failed to fetch price data for {symbol} or data is empty.")

    except Exception as e:
        print(f"An error occurred for {symbol}: {e}")

# Define a job to run your code every 2 minutes
def job():
    timestamp = int(time.time())
    timestamp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    print(f"{timestamp_str} - Running job...")
    symbols = ['TMF', 'IWM', 'XLK']  # Add more symbols as needed
#    with Pool(processes=len(symbols)) as pool:
#        pool.map(process_symbol, symbols)
    for symbol in symbols:
        process_symbol(symbol)
    timestamp = int(time.time())
    timestamp_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
    print(f"{timestamp_str} - Job completed.")

def writeProp():
    # Generate a key for encryption (do this only once and securely store the key)
    key = Fernet.generate_key()

    # Save the key to a file for later use
    with open('encryption_key.key', 'wb') as key_file:
        key_file.write(key)

    # Encrypt a password
    cipher_suite = Fernet(key)
    encrypted_password = cipher_suite.encrypt(b'password')

    # Save the encrypted password to a config file
    config = configparser.ConfigParser()
    config['Credentials'] = {'encrypted_password': encrypted_password.decode('utf-8')}
    config['Credentials'] = {'user': 'username'}

    with open('config.ini', 'w') as config_file:
        config.write(config_file)

def readProp():
    config = configparser.ConfigParser()

    # Load the key and the encrypted password
    with open('encryption_key.key', 'rb') as key_file:
        key = key_file.read()

    config.read('config.ini')
    encrypted_password_from_config = config.get('Credentials', 'encrypted_password')
    USER = config.get('Credentials', 'user')
    Brokerage_account_number = config.get('Credentials', 'Brokerage_account_number')

    # Decrypt the password
    cipher_suite = Fernet(key)
    PWD  = cipher_suite.decrypt(encrypted_password_from_config.encode('utf-8')).decode('utf-8')



if __name__ == '__main__':
    readProp()
    login()
    job()
    # Run the job immediately and then keep scheduling it every 8 minutes
    schedule.every(2).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(10)  # Sleep for 1 second to avoid high CPU usage

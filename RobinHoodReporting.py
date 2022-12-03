# Begin Code:
import pyotp
import csv
from datetime import date
import robin_stocks.robinhood as rs

# Assigning the correct code to run when we use login()
def login():
    rs.authentication.login(
        username = "EMAILID",
        password = "PWD",
        expiresIn = 86400,
        by_sms = True
    )

# Assigning the correct code to run when we use logout()
def logout():
    rs.authentication.logout()

login()
today = date.today().strftime('%Y-%m-%d')
# exports history to CSV file.
rs.export_completed_stock_orders(".")
rs.export_completed_option_orders(".")
print("-------------")
bank_trans_dict = rs.account.get_bank_transfers()
#print(bank_trans_dict)
print(type(bank_trans_dict))
print("-------------")
txf_filename = 'robin_txfrs-' + today + ".csv"
with open(txf_filename, 'w', newline='') as csvfile:
    fieldnames = ['amount', 'expected_landing_date']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for key in bank_trans_dict:
        print(key)
        print(type(key))
        print('Amount:',key['amount'],'Date:',key['expected_landing_date'])
        try:
            writer.writerow({'amount': key['amount'], 'expected_landing_date': key['expected_landing_date']})
            #writer.writerow(key)
        except csv.Error as e:
            print(e)


   
#print("-------------")
#print(rs.account.get_wire_transfers())
print("-------------")
holdings = rs.account.build_holdings()
print(type(holdings))
stk_prtfl_filename = 'robin_portfolio-' + today + ".csv"
with open(stk_prtfl_filename, 'w', newline='') as csvfile:
    fieldnamesprtfl = ['ticker', 'quantity', 'price', 'average_buy_price','profit']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnamesprtfl)
    writer.writeheader()
    for key2 in holdings:
        print(key2,'->',holdings[key2])
        print(type(holdings[key2]))
        try:
            writer.writerow({'ticker': key2, 'quantity': holdings[key2]['quantity'],'price':holdings[key2]['price'],'average_buy_price':holdings[key2]['average_buy_price'],'profit':holdings[key2]['equity_change']})
            #writer.writerow(key)
        except csv.Error as e:
            print(e)

#print(rs.account.get_dividends())

logout()

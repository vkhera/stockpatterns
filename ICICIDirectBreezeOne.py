from breeze_connect import BreezeConnect
import datetime
import schedule
import time

breeze = BreezeConnect(api_key="M4H88j96J1816&087u4804W874360A#1")
# Assigning the correct code to run when we use login()
def login():
    # Initialize SDK


    # Obtain your session key from https://api.icicidirect.com/apiuser/login?api_key=YOUR_API_KEY
    # Incase your api-key has special characters(like +,=,!) then encode the api key before using in the url as shown below.
    import urllib
    print("https://api.icicidirect.com/apiuser/login?api_key=" + urllib.parse.quote_plus(
        "M4H88j96J1816&087u4804W874360A#1"))

    # Generate Session
    breeze.generate_session(api_secret="2C611765^*1pS7g5o9p36J97480IWc25",
                            session_token="49210365")
    print("Session generated..")
    # Generate ISO8601 Date/DateTime String
    iso_date_string = datetime.datetime.strptime("28/11/2024","%d/%m/%Y").isoformat()[:10] + 'T05:30:00.000Z'
    iso_date_time_string = datetime.datetime.strptime("28/11/2024 23:59:59","%d/%m/%Y %H:%M:%S").isoformat()[:19] + '.000Z'


def job():
#   portholdings = breeze.get_portfolio_positions()
#    customer = breeze.get_customer_details(api_session="49210365")
    dmtholdings = breeze.get_demat_holdings()
    print(dmtholdings["Success"])
    # print(dmtholdings[1])
    for x in dmtholdings.keys():
        print(x)
    print("Portfolio fetched")
    current_time = datetime.datetime.now()
    print(current_time)





if __name__ == '__main__':

    login()
    job()
    # Run the job immediately and then keep scheduling it every 8 minutes
    schedule.every(3).minutes.do(job)

    while True:
         schedule.run_pending()
         time.sleep(200)  # Sleep for 1 second to avoid high CPU usage
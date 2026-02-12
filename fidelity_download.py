from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time
import json
import pandas as pd

# Load credentials from config file
with open('config.json', 'r') as f:
    config = json.load(f)

FIDELITY_USERNAME = config.get('fidelity_username', 'VIVEKKHERA108')
FIDELITY_PASSWORD = config.get('fidelity_password', '359524')

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument('--start-maximized')
# Uncomment the next line for headless mode
# chrome_options.add_argument('--headless')

# Set up the driver (make sure chromedriver is in your PATH)
driver = webdriver.Chrome(options=chrome_options)

try:
    # Go to Fidelity login page
    driver.get('https://digital.fidelity.com/prgw/digital/login/full-page')
    time.sleep(2)

    # Enter username
    user_input = driver.find_element(By.ID, 'dom-username-input')
    user_input.send_keys(FIDELITY_USERNAME)
    time.sleep(2)
    # Enter password
    pass_input = driver.find_element(By.ID, 'dom-pswd-input')
    pass_input.send_keys(FIDELITY_PASSWORD)
    time.sleep(2)
    pass_input.send_keys(Keys.RETURN)

    # Wait for 2FA or manual intervention if needed
    print('If prompted, please complete 2FA in the browser window...')
    time.sleep(30)  # Adjust as needed for 2FA

    # Navigate to Positions/Accounts page
    driver.get('https://digital.fidelity.com/ftgw/digital/portfolio/positions')
    time.sleep(5)

    # Wait for the table to load
    # You may need to adjust the selector based on Fidelity's page structure
#    table = driver.find_element(By.TAG_NAME, 'table')
#    html = table.get_attribute('outerHTML')
    tbl = driver.find_element(By.ID, 'kebabmenuitem-download')
    tbl.click()
    time.sleep(5)

    # Save the table HTML for further processing
#    with open('fidelity_holdings.html', 'w', encoding='utf-8') as f:
#        f.write(html)
#    print('Holdings table saved as fidelity_holdings.html.')

    # Convert HTML table to CSV using pandas
#    dfs = pd.read_html(html)
#    if dfs:
#        df = dfs[0]
#        df.to_csv('fidelity_holdings.csv', index=False)
#        print('Holdings table also saved as fidelity_holdings.csv.')
#    else:
#        print('No table found to convert to CSV.')

finally:
    driver.quit()

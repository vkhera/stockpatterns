import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from otp_reader import OTPReader

# --- User Config ---
ICICI_USERNAME = "your_icici_username"
ICICI_PASSWORD = "your_icici_password"
GMAIL_USER = "your_gmail@gmail.com"
GMAIL_APP_PASSWORD = "your_gmail_app_password"
IMAP_SERVER = "imap.gmail.com"
IMAP_FOLDER = "INBOX"
OTP_EMAIL_SUBJECT = "OTP for login to your ICICI Direct account"
OTP_SENDER = "service@icicisecurities.com"  # Adjust if needed

def main():
    driver = webdriver.Chrome()  # Or use webdriver.Firefox()
    driver.get("https://www.icicidirect.com/")
    time.sleep(2)
    # Click login button if needed, then fill username/password
    driver.find_element(By.ID, "txtUserId").send_keys(ICICI_USERNAME)
    driver.find_element(By.ID, "txtPassword").send_keys(ICICI_PASSWORD)
    driver.find_element(By.ID, "btnLogin").click()
    time.sleep(2)
    # Wait for OTP input to appear
    otp_reader = OTPReader(
        gmail_user=GMAIL_USER,
        gmail_app_password=GMAIL_APP_PASSWORD,
        imap_server=IMAP_SERVER,
        imap_folder=IMAP_FOLDER,
        otp_email_subject=OTP_EMAIL_SUBJECT,
        otp_sender=OTP_SENDER
    )
    try:
        otp = otp_reader.get_otp_with_retries(retries=3, delay=15)
    except Exception as e:
        print(f"OTP not found in Gmail: {e}")
        driver.quit()
        return
    driver.find_element(By.ID, "txtOtp").send_keys(otp)
    driver.find_element(By.ID, "btnSubmitOtp").click()
    print("Logged in! Navigating to portfolio...")
    # Add navigation to portfolio screen as needed
    # driver.get("https://...")  # Portfolio URL
    # time.sleep(5)
    # driver.quit()

if __name__ == "__main__":
    main()
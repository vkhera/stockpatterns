import imaplib
import email
import re

class OTPReader:
    def __init__(self, gmail_user, gmail_app_password, imap_server, imap_folder, otp_email_subject, otp_sender):
        self.gmail_user = gmail_user
        self.gmail_app_password = gmail_app_password
        self.imap_server = imap_server
        self.imap_folder = imap_folder
        self.otp_email_subject = otp_email_subject
        self.otp_sender = otp_sender

    def get_latest_otp(self):
        mail = imaplib.IMAP4_SSL(self.imap_server)
        mail.login(self.gmail_user, self.gmail_app_password)
        mail.select(self.imap_folder)
        # Search for unread emails from ICICI Direct with OTP in subject
        status, messages = mail.search(None, f'(UNSEEN FROM "{self.otp_sender}" SUBJECT "{self.otp_email_subject}")')
        mail_ids = messages[0].split()
        if not mail_ids:
            # Try searching all emails if no unread found
            status, messages = mail.search(None, f'(FROM "{self.otp_sender}" SUBJECT "{self.otp_email_subject}")')
            mail_ids = messages[0].split()
        if not mail_ids:
            raise Exception("No OTP email found.")
        latest_email_id = mail_ids[-1]
        status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
        msg = email.message_from_bytes(msg_data[0][1])
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True).decode()
                    break
        else:
            body = msg.get_payload(decode=True).decode()
        otp_match = re.search(r"\\b(\\d{6})\\b", body)
        if otp_match:
            return otp_match.group(1)
        else:
            raise Exception("OTP not found in email.")

    def get_otp_with_retries(self, retries=3, delay=15):
        import time
        for attempt in range(retries):
            try:
                return self.get_latest_otp()
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    raise e

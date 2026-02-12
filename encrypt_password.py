from cryptography.fernet import Fernet
import getpass
import json

# Generate a key and save it for future use
key = Fernet.generate_key()
fernet = Fernet(key)


# Robinhood credentials (optional, can be left blank if not used)
username = input("Enter your Robinhood username (or leave blank): ")
password = getpass.getpass("Enter your Robinhood password (or leave blank): ")
if password:
    enc_password = fernet.encrypt(password.encode()).decode()
else:
    enc_password = ""

print("Enter MySQL connection details:")
mysql_host = input("MySQL host: ")
mysql_user = input("MySQL username: ")
mysql_db = input("MySQL database: ")
mysql_password = getpass.getpass("MySQL password: ")

# Encrypt the MySQL password
enc_mysql_password = fernet.encrypt(mysql_password.encode()).decode()

# Save to config.json
config = {
    "username": username,
    "password": enc_password,
    "key": key.decode(),
    "mysql_host": mysql_host,
    "mysql_user": mysql_user,
    "mysql_db": mysql_db,
    "mysql_password": enc_mysql_password
}

with open("config.json", "w") as f:
    json.dump(config, f, indent=2)

print("Credentials (Robinhood and MySQL) encrypted and saved to config.json.")

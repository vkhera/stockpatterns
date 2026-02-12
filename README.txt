Instructions to run on Raspberry Pi 3:

1. Unzip all files into a folder on your Raspberry Pi.
2. Open a terminal in that folder.
3. Run:
   pip3 install -r requirements.txt
4. Edit config.json with your Robinhood credentials (encrypted using encrypt_password.py if needed).
5. Run:
   python3 main.py

Files included:
- main.py
- requirements.txt
- config.json (edit as needed)
- encrypt_password.py (if you want to update credentials)
- Any CSVs you want to process

Python 3 must be installed on your Raspberry Pi.

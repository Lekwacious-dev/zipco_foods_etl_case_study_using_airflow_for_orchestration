import pandas as pd

# Data Extraction
def run_extraction():
    try:
        data = pd.read_csv('zipco_transaction.csv')
    except Exception as e:
        print(f"An Error occured: {e}")
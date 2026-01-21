# Import Necessary Libraries

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os


def run_loading():
    # Load the dataset
    data =  pd.read_csv(r'cleaneddata.csv')
    products =  pd.read_csv(r'products.csv')
    customers =  pd.read_csv(r'customers.csv')
    staffs =  pd.read_csv(r'staffs.csv')
    weather =  pd.read_csv(r'weather.csv')
    transactions =  pd.read_csv(r'transactions.csv')
    # Load the environment from the .env files
    load_dotenv()

    connect_str = os.getenv('AZURE_CONNECTION_STRING_VALUE')
    container_name = os.getenv('CONTAINER_NAME')


    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # Load data to Azure Blob Storage
    files = [
        (data, 'rawdata/cleaned_zipco_transaction.csv'),
        (products, 'cleaneddata/products.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staffs, 'cleaneddata/staffs.csv'),
        (weather, 'cleaneddata/weather.csv'),
        (transactions, 'cleaneddata/transactions.csv')

    ]

    for file, blob_name in files:
        blob_client =container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into Azure Blob Storage')
import logging
import azure.functions as func
import yaml
from requests.auth import HTTPBasicAuth
import requests
import os
from azure.storage.blob import BlobClient

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def zones_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Grabbing secrets')
    api_key = os.environ['api_key_id']
    api_secret = os.environ['api_key_secret']
    storage_token = os.environ['blob_storage_token']

    logging.info('Generating basic api auth object')
    nyc_open_auth = HTTPBasicAuth(api_key, api_secret)

    url = 'https://data.cityofnewyork.us/resource/755u-8jsi.csv'

    rsp = requests.request('get', url, auth=nyc_open_auth)

    blob = BlobClient(account_url='https://oecapstorage.blob.core.windows.net', container_name=r'raw', blob_name='taxi_zones.csv', credential=storage_token)
    logging.info(f'Writing csv file in /raw/taxi_zones.csv')
    blob.uploadBlob(rsp.content.decode('utf-8'))


    logging.info('Python timer trigger function executed.')
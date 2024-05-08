import logging
import azure.functions as func
from requests.auth import HTTPBasicAuth
import requests
import os
from io import StringIO
from stream_unzip import stream_unzip
from azure.storage.blob import BlobClient


app = func.FunctionApp()


@app.schedule(schedule="0 20 23 * * *", arg_name="myTimer",
              run_on_startup=True, use_monitor=False)
def zones_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Starting timer triggered function')
        logging.info('Grabbing secrets')
        api_key = os.environ['api_key_id']
        api_secret = os.environ['api_key_secret']
        storage_token = os.environ['blob_storage_token']

        logging.info('Generating basic api auth object')
        nyc_open_auth = HTTPBasicAuth(api_key, api_secret)

        url = 'https://data.cityofnewyork.us/resource/755u-8jsi.csv'

        rsp = requests.request('get', url, auth=nyc_open_auth)
        location = 'https://oecapstorage.blob.core.windows.net'
        blob = BlobClient(account_url=location,
                          container_name=r'raw',
                          blob_name='taxi_zones.csv',
                          credential=storage_token)
        logging.info('Writing csv file in /raw/taxi_zones.csv')
        blob.uploadBlob(rsp.content.decode('utf-8'))

    logging.info('Python timer trigger function executed.')


@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer",
              run_on_startup=True, use_monitor=False)
def bike_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Process Started')
        storage_token = os.environ['blob_storage_token']

        def yield_chunks(year):
            s = requests.Session()
            url = f'https://s3.amazonaws.com/tripdata/{year}-citibike-tripdata.zip' # noqa
            with s.request('get', url,
                           stream=True) as rsp:
                yield from rsp.iter_content(chunk_size=262144)

        files_names = []
        for year in [2014]:
            for file_name, _, chunk in stream_unzip(yield_chunks(year)):
                str_file = file_name.decode('utf-8')

                if '.csv' not in str_file or 'MAC' in str_file:
                    for _ in chunk:
                        pass
                    continue
                elif str_file in files_names:
                    pass
                else:
                    files_names.append(str_file)
                    logging.info(f'Beginning extraction of {str_file}')
                with StringIO() as file:
                    for chunk in chunk:
                        file.write(chunk.decode())
                    file.seek(0)
                    location = 'https://oecapstorage.blob.core.windows.net'
                    blob = BlobClient(account_url=location,
                                      container_name=r'raw',
                                      blob_name=str_file,
                                      credential=storage_token)
                    logging.info('Writing csv file in /raw/taxi_zones.csv')
                    blob.uploadBlob(file)

    logging.info('Python timer trigger function executed.')

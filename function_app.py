import logging
import azure.functions as func
from requests.auth import HTTPBasicAuth
import requests
import aiohttp
from aiohttp import BasicAuth
import asyncio
import os
from io import BytesIO
from stream_unzip import stream_unzip
from azure.storage.blob import BlobClient


app = func.FunctionApp()


@app.schedule(schedule="0 0 0 1 * *", arg_name="myTimer",
              run_on_startup=False, use_monitor=False)
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
    blob.upload_blob(rsp.content.decode('utf-8'), overwrite=True)

    logging.info('Python timer trigger function executed.')


@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer",
              run_on_startup=False, use_monitor=False)
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
            with BytesIO() as file:
                for chunk in chunk:
                    file.write(chunk)
                file.seek(0)
                location = 'https://oecapstorage.blob.core.windows.net'
                blob = BlobClient(account_url=location,
                                  container_name=r'raw',
                                  blob_name=str_file,
                                  credential=storage_token)
                logging.info('Writing csv file in /raw/taxi_zones.csv')
                blob.upload_blob(file.getvalue(), overwrite=True)

    logging.info('Python timer trigger function executed.')


@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer",
              run_on_startup=False, use_monitor=False)
def green_taxi_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Process Started')
    logging.info('Grabbing secrets')

    api_key = os.environ['api_key_id']
    api_secret = os.environ['api_key_secret']
    storage_token = os.environ['blob_storage_token']

    base_url = 'https://data.cityofnewyork.us/resource/2np7-5jsg.csv'

    auth = HTTPBasicAuth(api_key, api_secret)

    offset = 0
    limit = 50000
    url = base_url+'?$offset={offset}&$limit={limit}'
    n = 0
    while True:
        rsp = requests.request(method='get',
                               url=url.format(offset=offset+limit*n,
                                              limit=limit),
                               auth=auth)
        n += 1
        resultset = [*rsp.iter_lines()]
        if not rsp.ok or len(resultset) <= 1:
            break

        location = 'https://oecapstorage.blob.core.windows.net'
        filename = f'green_taxi/trips_2014_{n}.csv'
        blob = BlobClient(account_url=location,
                          container_name=r'raw',
                          blob_name=filename,
                          credential=storage_token)
        logging.info(f'Writing csv file in /raw/{filename}')
        blob.upload_blob(rsp.content.decode('utf-8'), overwrite=True)

    logging.info('Starting process')


@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer",
              run_on_startup=True, use_monitor=False)
async def yellow_taxi_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Process Started')

    storage_token = os.environ['blob_storage_token']

    offset = 0
    limit = 50000
    i = 0

    location = 'https://oecapstorage.blob.core.windows.net'
    filename = 'yellow_taxi/trips_2014_{}.csv'

    while True:
        tasks = [
            asyncio.create_task(
                taxi_trip_api_call('gkne-dk5s',
                                   offset=offset+limit*n,
                                   limit=limit) for n in range(10)
            )
        ]
        offset += limit*10

        finished, _ = await asyncio.wait(tasks)

        for res in finished:
            logging.info(f'Writing csv file in /raw/{filename.format(i)}')
            blob = BlobClient(account_url=location,
                              container_name=r'raw',
                              blob_name=filename,
                              credential=storage_token)
            blob.upload_blob(res.result().content.decode('utf-8'),
                             overwrite=True)
            i += 1

        last_res = finished[-1]

        if len([*last_res.result().iter_lines()]) <= 1:
            logging.info(f'Process finished, {i} files written to blob storage')
            break


async def taxi_trip_api_call(resource, offset, limit):

    logging.info('Grabbing secrets')

    api_key = os.environ['api_key_id']
    api_secret = os.environ['api_key_secret']
    auth = BasicAuth(api_key, api_secret)

    url = f'https://data.cityofnewyork.us/resource/{resource}.csv?$offset={offset}&$limit={limit}'

    async with aiohttp.ClientSession() as client:
        rsp = await client.request(method='get',
                                   url=url,
                                   auth=auth)

    return rsp

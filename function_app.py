import logging
import azure.functions as func
import yaml
from requests.auth import HTTPBasicAuth
import requests
import os

app = func.FunctionApp()

@app.schedule(schedule="0 0 0 1 1 *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def zones_pull(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    api_key = os.environ['api_key_id']
    api_secret = os.environ['api_key_secret']
    logging.info('Python timer trigger function executed.')
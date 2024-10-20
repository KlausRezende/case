import requests
import json 
import logging
from libs.log import notification_discord
from datetime import datetime

url_api="https://api.openbrewerydb.org/v1/breweries"

response = requests.get(url_api)

data = response.json()

with open('/opt/airflow/bronze_layer/data_raw.json', 'w+') as f:
    json.dump(data, f)



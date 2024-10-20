import requests
import json 
import logging
from libs.log import notification_discord
from datetime import datetime

url_api="https://api.openbrewerydb.org/v1/breweries"

response = requests.get(url_api)

# try:
with open('/opt/airflow/bronze_layer/data_raw.json', 'w+') as f:
    json.dump(response.json(), f)
# except:
#     current_date = datetime.now()
#     message = f"The data extraction for {current_date} failed, alerting the data engineering team"
#     notification_discord(message)



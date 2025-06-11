from requests import Request, Session
import pandas as pd
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime


import json
import datetime
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

def get_currentETF_data():
    apikey = "SOSO-dd21c616213f4ea99dccdfa2f0ad625d"

    url = "https://api.sosovalue.xyz/openapi/v2/etf/currentEtfDataMetrics"
    payload = {
        "type": "us-btc-spot"
    }

    path = "C:/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/sosovalue" + \
        f"/BTC_etf_daily_{datetime.datetime.now().strftime('%Y%m%d')}.json"

    headers = {
        'Content-Type': 'application/json',
        'x-soso-api-key': apikey,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.post(url, data=json.dumps(payload))
        response.raise_for_status()  # để raise lỗi nếu mã lỗi HTTP
        etf_data = response.json()

        with open(path, 'w') as outfile:
            json.dump(etf_data, outfile, indent=4)

        print("Data saved to:", path)
        print(etf_data)

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print("❌ Request failed:", e)

    except Exception as ex:
        print("❌ Other error:", ex)



def get_historicalETF_data():
    apikey = "SOSO-dd21c616213f4ea99dccdfa2f0ad625d"

    url = "https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart"
    payload = {
        "type": "us-btc-spot"
    }

    path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/sosovalue/historical" + \
        f"/BTC_etf_total_historical.json"

    headers = {
        'Content-Type': 'application/json',
        'x-soso-api-key': apikey,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.post(url, data=json.dumps(payload))
        response.raise_for_status()  # để raise lỗi nếu mã lỗi HTTP
        etf_data = response.json()

        with open(path, 'w') as outfile:
            json.dump(etf_data, outfile, indent=4)

        print("Data saved to:", path)
        print(etf_data)

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print("❌ Request failed:", e)

    except Exception as ex:
        print("❌ Other error:", ex)




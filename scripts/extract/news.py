apikey = 'NR59H1H1GHPHO7ZP'
from requests import Request, Session
import pandas as pd
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime

# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
import json
import time
import datetime
from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

def get_daily_news():
    apikey = 'NR59H1H1GHPHO7ZP'
    session = Session()

    # Tính ngày hôm qua
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    time_from = yesterday.replace(hour=0, minute=0).strftime("%Y%m%dT%H%M")
    time_to = yesterday.replace(hour=23, minute=59).strftime("%Y%m%dT%H%M")

    url = f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=COIN,CRYPTO:BTC,FOREX:USD&time_from={time_from}&time_to={time_to}&apikey={apikey}'
    path = f"/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/news/news_data_yesterday_{yesterday.strftime('%Y-%m-%d')}.json"

    try:
        response = session.get(url)
        response.raise_for_status()
        data = response.json()

        with open(path, 'w') as f:
            json.dump(data, f, indent=4)

        print(f"✅ Data for {yesterday.strftime('%Y-%m-%d')} saved to {path}")
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(f"❌ Request error: {e}")
    except Exception as ex:
        print(f"❌ Unexpected error: {ex}")

if __name__ == "__main__":
    get_daily_news()

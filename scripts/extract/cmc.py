
from requests import Request, Session
import pandas as pd
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import datetime
import os

def get_category_data():
  apikey = '87d465c9-96df-497e-be9c-868463de43c1'
  url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/categories'
  dir_path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/categories"
  filename = f"categories_data_daily{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
  path = os.path.join(dir_path, filename)
  parameters = {
    # 'id':"605e2ce9d41eae1066535f7c",
    'start':'1',
    'limit':'5000',
  }
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': apikey,
  }
  session = Session()
  session.headers.update(headers)

  try:
    response = session.get(url, params=parameters)
    category_data = json.loads(response.text)

    with open(path, 'w') as outfile:
      outfile.write(json.dumps(category_data, indent=4))

    print("Data saved to", path)
    print(category_data)
    
  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)




def get_IDmap_data():
  apikey = '87d465c9-96df-497e-be9c-868463de43c1'
  url ='https://pro-api.coinmarketcap.com/v2/cryptocurrency/info'
  dir_path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/idmap"
  filename = f"idmap_data_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
  path = os.path.join(dir_path, filename)
  parameters = {
    # 'id':"605e2ce9d41eae1066535f7c",
    # 'start':'1',
    # 'limit':'5000',
    'id': f"{','.join(map(str, range(1, 1000)))}",
    'skip_invalid': 'true',
  }
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': apikey,
  }
  session = Session()
  session.headers.update(headers)

  try:
    response = session.get(url, params=parameters)
    category_data = json.loads(response.text)

    with open(path, 'w') as outfile:
      outfile.write(json.dumps(category_data, indent=4))

    print("Data saved to", path)
    print(category_data)
    
  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)


def get_cmc100_daily_data():
  apikey = '87d465c9-96df-497e-be9c-868463de43c1'
  url = 'https://pro-api.coinmarketcap.com/v3/index/cmc100-latest'
  dir_path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/cmc100"
  filename = f"cmc100_data_daily{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
  path = os.path.join(dir_path, filename)
  # parameters = {
  #   'time_start': '2024-05-10',
  #   'time_end': '2024-05-20',
  #   'interval': 'daily',
  # }
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': apikey,
  }

  session = Session()
  session.headers.update(headers)

  try:
    response = session.get(url)
    cmc100_data = json.loads(response.text)

    with open(path, 'w') as outfile:
      outfile.write(json.dumps(cmc100_data, indent=4))

    print("Data saved to", path)
    print(cmc100_data)
  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)




def get_fng_daily():
    apikey = '87d465c9-96df-497e-be9c-868463de43c1'
    dir_path = "/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/data/raw/coinmarketcap/fng"
    filename = f"fng_data_daily{datetime.datetime.now().strftime('%Y-%m-%d')}.json"
    path = os.path.join(dir_path, filename)
    url = 'https://pro-api.coinmarketcap.com/v3/fear-and-greed/latest'
    parameters = {

    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': apikey,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url)
        FnG_data = json.loads(response.text)

        with open(path, 'w') as outfile:
            outfile.write(json.dumps(FnG_data, indent=4))

        print("Data saved to", path)
        print(FnG_data)
        
    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)



if __name__ == "__main__":
    get_IDmap_data()
    get_category_data()
    get_cmc100_daily_data()
    get_fng_daily()
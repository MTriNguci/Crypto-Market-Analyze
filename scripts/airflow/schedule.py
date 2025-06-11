from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/scripts")
# sys.path.append("/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/scripts/extract")
# sys.path.append("/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/scripts/load")
# sys.path.append("/mnt/c/Users/Admin/OneDrive - VNU-HCMUS/Documents/K1N4/E2E/ETL/scripts/transform")
# Import các hàm extract từ scripts của bạn
from extract.cmc import get_IDmap_data, get_category_data, get_cmc100_daily_data, get_fng_daily
from extract.news import get_daily_news
from extract.sosovalue import get_historicalETF_data

from load.load_json_to_hdfs import load_api_to_parquet

from transform.transform_to_dw_idmap import transform_idmap
from transform.transform_to_dw_categories import transform_categories
from transform.transform_to_dw_cmc100 import transform_cmc100
from transform.transform_to_dw_fng import transform_fng
from transform.transform_to_dw_news import transform_news
from transform.transform_to_dw_btcetf import transform_btcetf

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'timeout': 1800,
}

# DAG định nghĩa
with DAG(
    dag_id='elt_cryptomarket_pipeline',
    default_args=default_args,
    description='ETL for crypto market data',
    start_date=datetime(2025, 5, 15),
    schedule_interval='@daily',
    catchup=False,
    tags=['crypto', 'elt'],
) as dag:

    # ==== Extract Tasks ====
    extract_idmap = PythonOperator(
        task_id='extract_idmap',
        python_callable=get_IDmap_data,
    )

    extract_category = PythonOperator(
        task_id='extract_category',
        python_callable=get_category_data,
    )

    extract_cmc100 = PythonOperator(
        task_id='extract_cmc100',
        python_callable=get_cmc100_daily_data,
    )

    extract_fng = PythonOperator(
        task_id='extract_fng',
        python_callable=get_fng_daily,
    )

    extract_news = PythonOperator(
        task_id='extract_news',
        python_callable=get_daily_news,
    )

    extract_etf = PythonOperator(
        task_id='extract_etf',
        python_callable=get_historicalETF_data,
    )

    # ==== Load Task ====
    load_all_to_hdfs = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=load_api_to_parquet,
    )

    # ==== Transform Tasks ====
    transform_idmap_task = PythonOperator(
        task_id='transform_idmap',
        python_callable=transform_idmap,
    )

    transform_categories_task = PythonOperator(
        task_id='transform_categories',
        python_callable=transform_categories,
    )

    transform_cmc100_task = PythonOperator(
        task_id='transform_cmc100',
        python_callable=transform_cmc100,
    )

    transform_fng_task = PythonOperator(
        task_id='transform_fng',
        python_callable=transform_fng,
    )

    transform_news_task = PythonOperator(
        task_id='transform_news',
        python_callable=transform_news,
    )

    transform_btc_etf_task = PythonOperator(
        task_id='transform_btc_etf',
        python_callable=transform_btcetf,
    )

    # ==== Task Dependencies ====
    [extract_idmap, extract_category, extract_cmc100, extract_fng, extract_news, extract_etf] >> load_all_to_hdfs

    load_all_to_hdfs >> [
        transform_idmap_task,
        transform_categories_task,
        transform_cmc100_task,
        transform_fng_task,
        transform_news_task,
        transform_btc_etf_task,
    ]

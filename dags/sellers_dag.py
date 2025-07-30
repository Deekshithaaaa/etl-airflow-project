from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
import os
import hashlib
from requests.auth import HTTPBasicAuth
from psycopg2.extras import execute_values

# Use environment variables for DB config or fallback to defaults
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_NAME = os.getenv("DB_NAME", "airflow")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASSWORD = os.getenv("DB_PASSWORD", "airflow")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def calculate_md5_hash(*args):
    """
    Calculate MD5 hash from concatenated string of all args.
    None is treated as empty string.
    """
    concat_str = ''.join([str(arg) if arg is not None else '' for arg in args])
    return hashlib.md5(concat_str.encode('utf-8')).hexdigest()

def fetch_and_load_sellers():
    url = "http://34.16.77.121:1515/sellers/"
    auth = HTTPBasicAuth("student1", "pass123")
    
    response = requests.get(url, auth=auth)
    response.raise_for_status()
    
    sellers = response.json()
    logging.info(f"✅ Total Sellers Records: {len(sellers)}")
    
    insert_query = """
        INSERT INTO sellers (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state,
            md5_hash
        ) VALUES %s
        ON CONFLICT (seller_id) DO UPDATE
        SET seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
            seller_city = EXCLUDED.seller_city,
            seller_state = EXCLUDED.seller_state,
            md5_hash = EXCLUDED.md5_hash;
    """

    values = []
    for seller in sellers:
        md5_hash = calculate_md5_hash(
            seller.get("seller_id"),
            seller.get("seller_zip_code_prefix"),
            seller.get("seller_city"),
            seller.get("seller_state")
        )
        values.append((
            seller.get("seller_id"),
            seller.get("seller_zip_code_prefix"),
            seller.get("seller_city"),
            seller.get("seller_state"),
            md5_hash
        ))

    try:
        with psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        ) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
                conn.commit()
        logging.info("✅ Sellers successfully loaded into PostgreSQL!")
    except Exception as e:
        logging.error(f"❌ Error loading sellers into PostgreSQL: {e}")
        raise

with DAG(
    dag_id='sellers_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline']
) as dag:

    fetch_and_load_task = PythonOperator(
        task_id='fetch_and_load_sellers',
        python_callable=fetch_and_load_sellers
    )

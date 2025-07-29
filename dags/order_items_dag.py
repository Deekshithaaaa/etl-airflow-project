from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import logging

# API Info
API_URL = "http://34.16.77.121:1515/order_items/"
USERNAME = "student1"
PASSWORD = "pass123"

# PostgreSQL Config
DB_CONFIG = {
    'host': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

def format_datetime(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None

def fetch_and_load_order_items():
    logging.info("🚀 Fetching order items from API...")
    response = requests.get(API_URL, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    logging.info(f"🔎 Status Code: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"❌ API Request Failed: {response.status_code} - {response.text}")

    data = response.json()
    logging.info(f"✅ Total Order Items Records: {len(data)}")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for item in data:
        cursor.execute("""
            INSERT INTO order_items (
                order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, order_item_id) DO UPDATE
            SET product_id = EXCLUDED.product_id,
                seller_id = EXCLUDED.seller_id,
                shipping_limit_date = EXCLUDED.shipping_limit_date,
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value
        """, (
            item.get("order_id"),
            item.get("order_item_id"),
            item.get("product_id"),
            item.get("seller_id"),
            format_datetime(item.get("shipping_limit_date")),
            item.get("price"),
            item.get("freight_value")
        ))

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("✅ Order items successfully loaded into PostgreSQL!")

with DAG(
    dag_id='order_items_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    fetch_and_load_order_items_task = PythonOperator(
        task_id='fetch_and_load_order_items',
        python_callable=fetch_and_load_order_items
    )

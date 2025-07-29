from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import logging

API_URL = "http://34.16.77.121:1515/orders/"
USERNAME = "student1"
PASSWORD = "pass123"

DB_CONFIG = {
    'host': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

def format_datetime(value):
    """Try parsing datetime with multiple formats."""
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None  # fallback if all formats fail

def fetch_and_load_orders():
    logging.info("🚀 Starting data fetch from API...")
    response = requests.get(API_URL, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    logging.info(f"🔎 Status Code: {response.status_code}")
    # logging.debug(f"🔎 Raw Response: {response.text[:200]}...")  # Uncomment if needed for debugging

    if response.status_code != 200:
        raise Exception(f"❌ API Request Failed: {response.status_code} - {response.text}")

    data = response.json()
    logging.info(f"✅ Successfully parsed JSON. Total Orders: {len(data)}")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        for order in data:
            cursor.execute("""
                INSERT INTO orders (order_id, product_id, customer_id, quantity, order_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE
                SET quantity = EXCLUDED.quantity,
                    order_date = EXCLUDED.order_date
            """, (
                order.get("order_id"),
                order.get("product_id"),
                order.get("customer_id"),
                order.get("quantity"),
                format_datetime(order.get("order_date"))
            ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    logging.info("✅ Data successfully loaded into PostgreSQL!")

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='orders_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args
) as dag:

    load_orders_task = PythonOperator(
        task_id='fetch_and_load_orders',
        python_callable=fetch_and_load_orders
    )

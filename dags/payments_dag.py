from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_load_payments():
    """
    Fetch payments data from API and load into the PostgreSQL payments table.
    """
    logging.info("🚀 Fetching payments data from API...")

    url = "http://34.16.77.121:1515/payments/"
    response = requests.get(url, auth=('student1', 'pass123'))

    logging.info(f"🔎 Status Code: {response.status_code}")

    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch data from Payments API: {response.status_code}")

    data = response.json()
    logging.info(f"✅ Total Payment Records: {len(data)}")

    # Update host here if your postgres service hostname is different
    db_config = {
        'host': 'postgres',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }

    # Use context managers to handle connection & cursor
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            insert_query = """
                INSERT INTO payments (
                    order_id,
                    payment_sequential,
                    payment_type,
                    payment_installments,
                    payment_value
                ) VALUES %s
                ON CONFLICT (order_id, payment_sequential) DO UPDATE
                SET payment_type = EXCLUDED.payment_type,
                    payment_installments = EXCLUDED.payment_installments,
                    payment_value = EXCLUDED.payment_value;
            """

            values = [
                (
                    payment.get("order_id"),
                    int(payment.get("payment_sequential", 0)),
                    payment.get("payment_type"),
                    int(payment.get("payment_installments", 0)),
                    float(payment.get("payment_value", 0.0))
                )
                for payment in data
            ]

            execute_values(cursor, insert_query, values)
            conn.commit()

    logging.info("✅ Payments successfully loaded into PostgreSQL!")

with DAG(
    dag_id='payments_dag',
    default_args=default_args,
    description='DAG to fetch and load payments data',
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['etl', 'payments'],
) as dag:

    fetch_and_load_payments_task = PythonOperator(
        task_id='fetch_and_load_payments',
        python_callable=fetch_and_load_payments
    )

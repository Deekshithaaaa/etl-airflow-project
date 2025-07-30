from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
from psycopg2.extras import execute_values
import hashlib

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def calculate_md5_hash(*args):
    """
    Calculate MD5 hash from concatenated string of all args.
    None is treated as empty string.
    """
    concat_str = ''.join([str(arg) if arg is not None else '' for arg in args])
    return hashlib.md5(concat_str.encode('utf-8')).hexdigest()

def fetch_and_load_payments():
    logging.info("🚀 Fetching payments data from API...")

    url = "http://34.16.77.121:1515/payments/"
    response = requests.get(url, auth=('student1', 'pass123'))

    logging.info(f"🔎 Status Code: {response.status_code}")

    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch data from Payments API: {response.status_code}")

    data = response.json()
    logging.info(f"✅ Total Payment Records: {len(data)}")

    db_config = {
        'host': 'postgres',
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            # Fetch existing md5 hashes for duplicate detection
            cursor.execute("SELECT md5_hash FROM payments")
            existing_hashes = set(row[0] for row in cursor.fetchall())

            insert_query = """
                INSERT INTO payments (
                    order_id,
                    payment_sequential,
                    payment_type,
                    payment_installments,
                    payment_value,
                    md5_hash
                ) VALUES %s
                ON CONFLICT (order_id, payment_sequential) DO UPDATE
                SET payment_type = EXCLUDED.payment_type,
                    payment_installments = EXCLUDED.payment_installments,
                    payment_value = EXCLUDED.payment_value,
                    md5_hash = EXCLUDED.md5_hash;
            """

            values = []
            for payment in data:
                order_id = payment.get("order_id")
                payment_sequential = int(payment.get("payment_sequential", 0))
                payment_type = payment.get("payment_type")
                payment_installments = int(payment.get("payment_installments", 0))
                payment_value = float(payment.get("payment_value", 0.0))

                # Calculate md5 hash on all relevant fields
                md5_hash = calculate_md5_hash(order_id, payment_sequential, payment_type, payment_installments, payment_value)

                if md5_hash not in existing_hashes:
                    values.append((
                        order_id,
                        payment_sequential,
                        payment_type,
                        payment_installments,
                        payment_value,
                        md5_hash
                    ))

            if values:
                execute_values(cursor, insert_query, values)
                conn.commit()
                logging.info(f"✅ Inserted {len(values)} new payments records into PostgreSQL!")
            else:
                logging.info("ℹ️ No new payment records to insert (all duplicates detected).")

with DAG(
    dag_id='payments_dag',
    default_args=default_args,
    description='DAG to fetch and load payments data with MD5 duplicate detection',
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['etl', 'payments'],
) as dag:

    fetch_and_load_payments_task = PythonOperator(
        task_id='fetch_and_load_payments',
        python_callable=fetch_and_load_payments
    )

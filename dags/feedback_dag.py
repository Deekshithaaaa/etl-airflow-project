from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import logging
import hashlib

# API and Auth Info
API_URL = "http://34.16.77.121:1515/feedback/"
USERNAME = "student1"
PASSWORD = "pass123"

# PostgreSQL connection info
DB_CONFIG = {
    'host': 'postgres',
    'user': 'airflow',
    'password': 'airflow',
    'dbname': 'airflow'
}

def calculate_md5_hash(*args):
    """
    Calculate MD5 hash from concatenated string of all args.
    None is treated as empty string.
    """
    concat_str = ''.join([str(arg) if arg is not None else '' for arg in args])
    return hashlib.md5(concat_str.encode('utf-8')).hexdigest()

def fetch_and_load_feedback():
    logging.info("🚀 Fetching feedback from API...")
    response = requests.get(API_URL, auth=HTTPBasicAuth(USERNAME, PASSWORD))
    logging.info(f"🔎 Status Code: {response.status_code}")
    logging.info(f"🔎 Response Preview: {response.text[:200]}")

    if response.status_code != 200:
        raise Exception(f"❌ API Request Failed: {response.status_code} - {response.text}")

    data = response.json()
    logging.info(f"✅ Total Feedback Records: {len(data)}")

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        for fb in data:
            feedback_id = fb.get("feedback_id")
            order_id = fb.get("order_id")
            customer_id = fb.get("customer_id")
            rating = fb.get("feedback_score")  # Fixed field mapping here
            comments = fb.get("comments")
            feedback_date = fb.get("feedback_date")
            if feedback_date:
                feedback_date = datetime.strptime(feedback_date, "%m/%d/%Y %H:%M")
            else:
                feedback_date = None
            
            md5_hash = calculate_md5_hash(feedback_id, order_id, customer_id, rating, comments, feedback_date)
            dv_load_timestamp = datetime.now()

            cursor.execute(
                """
                INSERT INTO feedback (feedback_id, order_id, customer_id, rating, comments, feedback_date, md5_hash, dv_load_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (feedback_id) DO UPDATE
                SET rating = EXCLUDED.rating,
                    comments = EXCLUDED.comments,
                    feedback_date = EXCLUDED.feedback_date,
                    md5_hash = EXCLUDED.md5_hash,
                    dv_load_timestamp = EXCLUDED.dv_load_timestamp
                """,
                (
                    feedback_id,
                    order_id,
                    customer_id,
                    rating,
                    comments,
                    feedback_date,
                    md5_hash,
                    dv_load_timestamp
                )
            )

        conn.commit()
        logging.info("✅ Feedback successfully loaded into PostgreSQL!")
    except Exception as e:
        logging.error(f"❌ Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

with DAG(
    dag_id='feedback_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['api', 'feedback'],
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    load_feedback_task = PythonOperator(
        task_id='fetch_and_load_feedback',
        python_callable=fetch_and_load_feedback
    )

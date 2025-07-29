from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import logging

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
            cursor.execute(
                """
                INSERT INTO feedback (feedback_id, order_id, customer_id, rating, comments, feedback_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (feedback_id) DO UPDATE
                SET rating = EXCLUDED.rating,
                    comments = EXCLUDED.comments,
                    feedback_date = EXCLUDED.feedback_date
                """,
                (
                    fb.get("feedback_id"),
                    fb.get("order_id"),
                    fb.get("customer_id"),
                    fb.get("rating"),
                    fb.get("comments"),
                    datetime.strptime(fb.get("feedback_date"), "%m/%d/%Y %H:%M") if fb.get("feedback_date") else None
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
    tags=['api', 'feedback']
) as dag:

    load_feedback_task = PythonOperator(
        task_id='fetch_and_load_feedback',
        python_callable=fetch_and_load_feedback
    )


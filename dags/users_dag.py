from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging
from requests.auth import HTTPBasicAuth
from psycopg2.extras import execute_values

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def fetch_and_load_users():
    url = "http://34.16.77.121:1515/users/"
    auth = HTTPBasicAuth('student1', 'pass123')
    
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch users: {e}")
        raise

    users = response.json()
    logging.info(f"✅ Total Users Records: {len(users)}")

    insert_query = """
        INSERT INTO users (user_name, email, first_name, last_name, signup_date)
        VALUES %s
        ON CONFLICT (user_name) DO NOTHING;
    """

    values = [
        (
            user.get("user_name"),
            user.get("email"),
            user.get("first_name"),
            user.get("last_name"),
            user.get("signup_date")
        )
        for user in users
    ]

    try:
        with psycopg2.connect(
            host="airflow-project-postgres-1",
            database="airflow",
            user="airflow",
            password="airflow"
        ) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, values)
                conn.commit()
        logging.info("✅ Users successfully loaded into PostgreSQL!")
    except Exception as e:
        logging.error(f"Failed to load users into DB: {e}")
        raise

with DAG(
    dag_id='users_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'users'],
) as dag:

    fetch_and_load_users_task = PythonOperator(
        task_id='fetch_and_load_users',
        python_callable=fetch_and_load_users
    )

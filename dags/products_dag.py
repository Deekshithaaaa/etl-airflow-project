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
}

def fetch_and_load_products():
    url = "http://34.16.77.121:1515/products/"
    response = requests.get(url, auth=HTTPBasicAuth('student1', 'pass123'))

    if response.status_code != 200:
        logging.error(f"❌ Failed to fetch products. Status Code: {response.status_code}")
        raise Exception(f"Failed to fetch products: HTTP {response.status_code}")

    products = response.json()
    logging.info(f"✅ Total Products: {len(products)}")

    db_config = {
        'host': 'postgres',  # change if needed
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }

    insert_query = """
        INSERT INTO products (
            product_id, product_category_name, product_name_length, product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm
        )
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE
        SET product_category_name = EXCLUDED.product_category_name,
            product_name_length = EXCLUDED.product_name_length,
            product_description_length = EXCLUDED.product_description_length,
            product_photos_qty = EXCLUDED.product_photos_qty,
            product_weight_g = EXCLUDED.product_weight_g,
            product_length_cm = EXCLUDED.product_length_cm,
            product_height_cm = EXCLUDED.product_height_cm,
            product_width_cm = EXCLUDED.product_width_cm;
    """

    values = [
        (
            product.get("product_id"),
            product.get("product_category_name"),
            product.get("product_name_length"),
            product.get("product_description_length"),
            product.get("product_photos_qty"),
            product.get("product_weight_g"),
            product.get("product_length_cm"),
            product.get("product_height_cm"),
            product.get("product_width_cm"),
        )
        for product in products
    ]

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, insert_query, values)
                conn.commit()
        logging.info("✅ Products successfully loaded into PostgreSQL!")
    except Exception as e:
        logging.error(f"❌ Error loading products into PostgreSQL: {e}")
        raise

with DAG(
    'products_dag',
    default_args=default_args,
    description='Fetch and load products data',
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_and_load_products_task = PythonOperator(
        task_id='fetch_and_load_products',
        python_callable=fetch_and_load_products
    )

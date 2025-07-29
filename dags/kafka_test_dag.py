from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BROKER = 'kafka:9092'  # use container hostname for internal Docker network
TOPIC = 'airflow-test-topic'

# Function to produce a message
def produce_message():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    producer.send(TOPIC, b'Hello from Airflow!')
    producer.flush()
    print("✅ Message sent to Kafka.")

# Function to consume the message
def consume_message():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='airflow-test-group'
    )
    print("✅ Waiting for message...")
    for message in consumer:
        print(f"📥 Received: {message.value.decode('utf-8')}")
        break  # Stop after receiving one message

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='kafka_test_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    produce_task = PythonOperator(
        task_id='produce_message',
        python_callable=produce_message
    )

    consume_task = PythonOperator(
        task_id='consume_message',
        python_callable=consume_message
    )

    produce_task >> consume_task

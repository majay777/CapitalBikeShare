from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import json
import requests

def produce_once():
    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    response = requests.get("https://gbfs.lyft.com/gbfs/2.3/bay/en/station_status.json")
    data = response.json()["data"]["stations"]

    for record in data:
        producer.send("bike_station_status", record)

    producer.flush()
    producer.close()

with DAG(
        dag_id="kafka_producer_dag",
        start_date=datetime(2024, 1, 1),
        schedule_interval="*/2 * * * *",  # every 2 minutes
        catchup=False,
) as dag:

    produce = PythonOperator(
        task_id="produce_kafka_data",
        python_callable=produce_once
    )

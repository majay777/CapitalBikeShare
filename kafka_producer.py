import json, time, requests
from kafka import KafkaProducer
import requests.exceptions as request_exceptions

server = 'localhost:9092'


def json_serializer(data1):
    return json.dumps(data1).encode('utf-8')







#
# producer = KafkaProducer(
#     bootstrap_servers=[server],
#     value_serializer=json_serializer
# )
#
# API_URL = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json"
#
# while True:
#     try:
#         response = requests.get(API_URL).json()
#         producer.send("events", response)  # send actual API data
#         producer.flush()
#         print("Message sent")
#     except request_exceptions.MissingSchema:
#         print(f"{API_URL} appears to be invalid url.")
#     except request_exceptions.ConnectionError:
#         print(f"Could not connect to {API_URL}")
#     time.sleep(60)

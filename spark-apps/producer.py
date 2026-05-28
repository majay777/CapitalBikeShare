# from kafka import KafkaProducer
# import json
# import time
#
# producer = KafkaProducer(
#     bootstrap_servers="kafka:9092",
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )
#
# print("Producer started")
#
# i = 0
# while True:
#     msg = {"msg": f"hello {i}"}
#     producer.send("events", msg)
#     producer.flush()
#     print("Sent:", msg)
#     i += 1
#     time.sleep(5)



import json, time, requests
from kafka import KafkaProducer
import requests.exceptions as request_exceptions

server = 'kafka:9092'


def json_serializer(data1):
    return json.dumps(data1).encode('utf-8')








producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=json_serializer
)

API_URL = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/station_status.json"

while True:
    try:
        response = requests.get(API_URL).json()
        producer.send("events", response)  # send actual API data
        producer.flush()
        print("Message sent")
    except request_exceptions.MissingSchema:
        print(f"{API_URL} appears to be invalid url.")
    except request_exceptions.ConnectionError:
        print(f"Could not connect to {API_URL}")
    time.sleep(60)

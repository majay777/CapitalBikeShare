import json, time, requests
from kafka import KafkaProducer
from datetime import datetime

TOPIC = "bike_station_status"
BROKER = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

GBFS_URL = "https://gbfs.lyft.com/gbfs/2.3/bay/en/station_status.json"

while True:
    data = requests.get(GBFS_URL).json()["data"]["stations"]

    for station in data:
        station["event_time"] = datetime.utcnow().isoformat()
        producer.send(TOPIC, station)
        producer.flush()
    print("📤 Sent station status batch")
    time.sleep(60)

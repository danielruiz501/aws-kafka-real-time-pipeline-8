import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer

from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_FILE = BASE_DIR / "data" / "sales_data.csv"


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def stream_sales_data():
    """Read sales data and publish each record to Kafka."""

    with open(DATA_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for sale in reader:
            producer.send(KAFKA_TOPIC, value=sale)

            print(f"Sent event: {sale['order_id']}")

            time.sleep(1)


if __name__ == "__main__":
    try:
        stream_sales_data()
    finally:
        producer.flush()
        producer.close()
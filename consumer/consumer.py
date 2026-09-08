import json
import boto3
from kafka import KafkaConsumer


# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "172.31.3.85:9092"
KAFKA_TOPIC = "sales-events"


# S3 configuration
S3_BUCKET = "daniel-kafka-sales-pipeline-8"
S3_PREFIX = "raw/"


# AWS S3 client
s3 = boto3.client("s3")


# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="sales-consumer-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True
)


print("Kafka Consumer started...")
print(f"Listening to topic: {KAFKA_TOPIC}")


try:
    for message in consumer:

        try:
            sale = json.loads(message.value.decode("utf-8"))

        except json.JSONDecodeError as error:
            print(
                f"Skipping invalid event: "
                f"{message.value.decode('utf-8')}"
            )
            print(f"Reason: {error}")
            continue

        try:
            order_id = sale["order_id"]

            print(f"Received event: {order_id}")

            file_name = f"{S3_PREFIX}{order_id}.json"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=file_name,
                Body=json.dumps(sale).encode("utf-8"),
                ContentType="application/json"
            )

            print(f"Uploaded to S3: {file_name}")

        except KeyError as error:
            print(f"Skipping event with missing field: {error}")
            continue

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
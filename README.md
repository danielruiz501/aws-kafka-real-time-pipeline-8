# Project 8 — Real-Time Sales Data Pipeline with Apache Kafka & AWS

**Real-time sales data streaming, processing, storage, and analytics using Apache Kafka and AWS.**

---

## 👨‍💻 Author

**Daniel Ruiz Lopez**

Junior Data Engineer

[LinkedIn](https://www.linkedin.com/in/danielruizl/) •
[GitHub](https://github.com/danielruiz501) •
[Email](mailto:danielruizlopez889@gmail.com)

---

## 🛠️ Technologies

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.1.2-black?logo=apachekafka)
![AWS](https://img.shields.io/badge/AWS-Cloud-orange?logo=amazonaws)
![Amazon EC2](https://img.shields.io/badge/Amazon%20EC2-Compute-orange?logo=amazonec2)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-Data%20Lake-red?logo=amazons3)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-ETL-purple?logo=amazonaws)
![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-SQL-blue?logo=amazonaws)
![AWS IAM](https://img.shields.io/badge/AWS%20IAM-Security-yellow?logo=amazonaws)
![Amazon CloudWatch](https://img.shields.io/badge/Amazon%20CloudWatch-Monitoring-orange?logo=amazonaws)

---

## 📌 Overview

This project implements an end-to-end real-time sales data pipeline using **Apache Kafka and AWS**.

The pipeline simulates e-commerce sales events from a synthetic dataset, publishes the events to an Apache Kafka topic, consumes them with Python, stores the raw JSON events in Amazon S3, transforms the data with AWS Glue, stores the processed data as compressed Parquet files, and enables analytical SQL queries using Amazon Athena.

The project demonstrates a complete modern data engineering workflow:

**Data Generation → Kafka Streaming → S3 Data Lake → ETL → Data Catalog → SQL Analytics**

> **Dataset Note:** The sales dataset used in this project is synthetic and was generated specifically to simulate e-commerce sales events for learning and portfolio purposes.

---

## 🏗️ Architecture

![Architecture](architecture/architecture.png)

### Data Flow

```text
Sales Data
    │
    ▼
Python Kafka Producer
    │
    ▼
Apache Kafka
    │
    ▼
Python Kafka Consumer
    │
    ▼
Amazon S3
Raw JSON Zone
    │
    ▼
AWS Glue ETL
    │
    ▼
Amazon S3
Processed Parquet Zone
    │
    ▼
AWS Glue Data Catalog
    │
    ▼
Amazon Athena
    │
    ▼
SQL Analytics
```

---

## 🔄 Pipeline Components

### 1. Data Source

The pipeline uses a synthetic e-commerce sales dataset.

The dataset contains sales events that are used to simulate a real-time streaming environment.

### 2. Apache Kafka

Apache Kafka is used as the real-time streaming platform for the sales events.

The project uses a Kafka topic called:

```text
sales-events
```

The topic receives sales events from the Python producer and makes them available to the Python consumer.

### 3. Python Kafka Producer

A Python producer reads the sales data and publishes each sales event to the Kafka topic.

The producer is responsible for sending the events to the Kafka streaming platform.

```text
Sales Dataset
     │
     ▼
Python Producer
     │
     ▼
Kafka Topic: sales-events
```

### 4. Python Kafka Consumer

A Python consumer subscribes to the `sales-events` Kafka topic and receives the incoming sales events.

The consumer processes the events and uploads the raw data to Amazon S3 for further processing.

### 5. Amazon S3 — Raw Zone

The Kafka consumer stores the incoming sales events in Amazon S3 as raw JSON data.

The raw zone preserves the original event data before any transformation is performed.

```text
Amazon S3
└── raw/
    └── sales events (JSON)
```

### 6. AWS Glue ETL

AWS Glue is used to transform the raw sales data stored in Amazon S3.

The ETL job processes the JSON data and converts it into an optimized Parquet format for analytical workloads.

```text
Raw JSON
   │
   ▼
AWS Glue ETL
   │
   ▼
Processed Parquet
```

### 7. Amazon S3 — Processed Zone

The transformed sales data is stored in Amazon S3 as compressed Parquet files.

Parquet provides an efficient columnar format for analytical workloads and helps optimize storage and query performance.

```text
Amazon S3
└── processed/
    └── sales data (Parquet + Snappy)
```

### 8. AWS Glue Data Catalog

AWS Glue Data Catalog stores the metadata of the processed sales dataset.

The catalog makes the dataset available to analytical services such as Amazon Athena.

### 9. Amazon Athena

Amazon Athena is used to query the processed sales data stored in Amazon S3 using SQL.

The processed Parquet dataset can be analyzed to obtain business insights such as total sales, revenue by country, and product performance.

Example analytical questions include:

- What are the total sales?
- Which products generate the most revenue?
- Which countries have the highest sales?
- What are the most popular products?

---

## 📊 SQL Analytics

The processed sales data can be queried in Amazon Athena using standard SQL.

Example query:

```sql
SELECT
    country,
    SUM(total_amount) AS total_sales
FROM sales
GROUP BY country
ORDER BY total_sales DESC;
```

This query calculates total sales by country and orders the results from highest to lowest sales.

---

## 📸 Project Evidence

### Kafka Topic

The `sales-events` Kafka topic is used to receive and stream sales events through the pipeline.

![Kafka Topic](screenshots/kafka-topic-created.png)

### Kafka Producer and Consumer

The Python producer publishes sales events to Kafka, while the consumer receives the events from the `sales-events` topic.

![Kafka Producer and Consumer](screenshots/kafka-producer-consumer-test.png)

### Kafka Sales Events

Sales events are published to the Kafka topic and consumed by the Python consumer.

![Kafka Sales Events](screenshots/kafka-sales-events.png)

### Kafka Consumer Uploading Events to S3

The Python Kafka consumer receives the sales events and uploads the raw JSON data to Amazon S3.

![Kafka Consumer Uploading Events to S3](screenshots/kafka-consumer-s3-upload.png)

### S3 Raw JSON Validation

The raw sales events are stored in Amazon S3 as JSON data before the ETL transformation.

![Kafka Consumer Uploading Events to S3](screenshots/kafka-consumer-s3-upload.png)

### AWS Glue ETL

AWS Glue transforms the raw sales data from JSON into an optimized Parquet format for analytical workloads.

![AWS Glue ETL](screenshots/glue-etl-processed.png)

### Athena Processed Data

The processed Parquet dataset is available for analysis through Amazon Athena.

![Athena Processed Data](screenshots/athena-processed-results.png)

### Athena Sales Analysis

SQL queries are used in Amazon Athena to analyze the processed sales data and generate business insights.

![Athena Sales Analysis](screenshots/athena-sales-analysis.png)

### Athena Sales by Country

Sales performance is analyzed by country using SQL queries in Amazon Athena.

![Athena Sales by Country](screenshots/athena-sales-by-country.png)

---

## 📁 Project Structure

```text
project-8-kafka-aws/
│
├── producer/
│   └── kafka_producer.py
│
├── consumer/
│   └── kafka_consumer.py
│
├── glue/
│   └── glue_etl.py
│
├── data/
│   └── sales.csv
│
├── kafka-topic-created.png
├── kafka-producer-consumer-test.png
├── kafka-sales-events.png
├── kafka-consumer-s3-upload.png
├── s3-raw-json-validation.png
├── glue-etl-job.png
├── athena-processed-data.png
├── athena-sales-analysis.png
├── athena-sales-by-country.png
│
├── architecture.png
├── README.md
└── LICENSE
```

---

## ☁️ AWS Services

| Service | Purpose |
|---|---|
| Amazon EC2 | Kafka infrastructure and Python applications |
| Amazon S3 | Data lake storage |
| AWS Glue | ETL processing |
| AWS Glue Data Catalog | Dataset metadata |
| Amazon Athena | SQL analytics |
| AWS IAM | Permissions and access control |
| Amazon CloudWatch | Monitoring and logs |

---

## 📈 Results

The project successfully demonstrates an end-to-end real-time sales data pipeline using Apache Kafka and AWS.

The pipeline is able to:

- Stream sales events using Apache Kafka
- Produce and consume events with Python
- Store raw JSON data in Amazon S3
- Transform data using AWS Glue
- Store processed data as Parquet
- Catalog the dataset using AWS Glue Data Catalog
- Query the processed data using Amazon Athena
- Perform SQL-based sales analysis
- Monitor the pipeline using Amazon CloudWatch

---

## 🚀 Future Improvements

Possible improvements for a production-oriented version include:

- Real-time monitoring and alerting with Amazon CloudWatch
- Automated AWS Glue job execution
- Data quality validation
- Partitioning the processed data for improved query performance
- Additional analytical queries and dashboards
- Amazon QuickSight dashboards
- Infrastructure as Code using Terraform
- CI/CD automation for the pipeline

---

## ✅ Status

**Completed — End-to-end real-time sales data pipeline with Apache Kafka and AWS.**

---

## 🎯 Key Takeaways

This project provided hands-on experience building a real-time data engineering pipeline using Apache Kafka and AWS.

Key skills demonstrated:

- Real-time data streaming with Apache Kafka
- Python-based data producers and consumers
- Event-driven data ingestion
- Amazon S3 data lake architecture
- ETL processing with AWS Glue
- Data transformation from JSON to Parquet
- Metadata management with AWS Glue Data Catalog
- SQL analytics with Amazon Athena
- AWS IAM permissions and access control
- CloudWatch monitoring and logging
- End-to-end data pipeline design
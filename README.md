# Real-Time Transaction Monitoring System

A real-time transaction monitoring system built using **Python, PySpark, Databricks, Kafka, Confluent Cloud, PostgreSQL, and SQL**.

## Overview

FinGuard processes financial transactions in real time and detects suspicious transactions using **spending limits and fraud watchlists**.

## Architecture

```text
Transaction Producer
        ↓
Confluent Cloud / Kafka
        ↓
Databricks Pipeline
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Fraud Detection
        ↓
Email Alert
```

## Databricks Pipeline

The Databricks pipeline manages the flow of streaming transaction data from **Kafka to the Bronze and Silver layers**. **PySpark Structured Streaming** is used to process the data.

## Data Layers

**Bronze Layer:** Stores raw transaction data received from Kafka with minimal transformation.

**Silver Layer:** Stores cleaned and transformed transaction data, which is used for fraud detection and further processing.

## Fraud Detection

Transactions are checked against **customer spending limits and fraud watchlists**. Suspicious transactions trigger automated email alerts.

## Technologies

* **Python** – Application logic
* **Kafka / Confluent Cloud** – Real-time data streaming
* **PySpark** – Data processing
* **Databricks** – Streaming pipeline
* **PostgreSQL** – Data storage
* **SQL** – Data querying

## Key Features

* Real-time transaction processing
* Kafka-based streaming
* Bronze/Silver data architecture
* Spending-limit fraud detection
* Fraud watchlist matching
* Automated email alerts
* Databricks Secrets for secure credentials

**Anshu Raj**

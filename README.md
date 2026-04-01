# 📘 Real‑Time Crypto Anomaly Detection Pipeline

This project builds a real‑time, cloud‑native system that collects live cryptocurrency trades, cleans and validates them, engineers meaningful features, and uses machine learning to detect abnormal or suspicious market behavior.  
It mirrors the type of monitoring systems used by exchanges, hedge funds, and fintech companies.

---

## 🔌 1. Live Data Ingestion (Coinbase WebSocket → Kafka)

A streaming service connects to the Coinbase WebSocket feed and sends every trade (price, volume, side, symbol, timestamp) into Kafka in real time.

**Why Kafka:**  
Kafka is built for high‑speed, real‑time data. It buffers spikes, prevents data loss, and decouples ingestion from downstream processing — exactly how real trading systems handle market data.

---

## 🐳 Docker‑Based Local Environment

All ingestion components (Kafka, Zookeeper, Producer, Consumer) run inside **Docker containers**.  
This ensures:

- consistent, reproducible environments  
- isolated services with clean networking  
- easy startup/shutdown of the entire pipeline  
- no dependency conflicts on your local machine  

Using Docker mirrors how real data engineering teams containerize ingestion and streaming workloads.

---

## 📥 2. RAW Data Layer (S3)

Kafka streams are written directly into an S3 RAW zone as JSON files.

**Why S3 RAW:**  
It stores the original data exactly as received, enabling replay, auditing, and reprocessing — a core principle of modern data lake design.

---

## 🧹 3. Glue Job 1 — Cleaning & Validation

This job prepares the data specifically for anomaly detection by:

- enforcing correct data types  
- removing invalid or impossible values  
- validating symbols and buy/sell fields  
- parsing timestamps  
- dropping malformed rows  

**Why this matters:**  
Anomaly detection models are sensitive to noise. Clean, consistent data ensures the model learns real market behavior, not ingestion errors.

---

## 🧠 4. Glue Job 2 — Feature Engineering + Anomaly Detection

Glue Job 2 transforms each trade into a richer feature set:

- **time features:** hour of day, day of week  
- **categorical encodings:** buy/sell, crypto symbol  
- **numeric features:** price, volume  

Then it trains an **Isolation Forest**, an unsupervised ML model widely used in finance to detect unusual or suspicious activity.

**What the model learns:**

- normal price/volume patterns  
- symbol‑specific behavior  
- typical buy/sell pressure  
- time‑of‑day and day‑of‑week patterns  

**What it outputs:**  
Anomaly scores that highlight trades that deviate from expected behavior.

The full ML pipeline (feature engineering + model) is saved to S3 for reuse.

---

## 🏛️ Optional: Snowflake Warehousing

The cleaned and/or anomaly‑scored data can be loaded into **Snowflake** for:

- fast SQL analytics  
- dashboarding  
- long‑term warehousing  
- BI and reporting  
- joining with other datasets  

Snowflake provides a scalable, low‑maintenance warehouse layer that complements the S3‑based data lake.

This allows analysts, quants, and data teams to run queries like:

- “Show me the top 1% most anomalous trades today.”  
- “Which symbols show the highest anomaly frequency?”  
- “Are anomalies clustering around certain hours?”  

Snowflake turns your anomaly detection pipeline into a fully queryable analytics platform.

---

## 🚨 Why Anomaly Detection Is Important

Crypto markets are volatile and prone to:

- manipulation  
- wash trading  
- sudden volume spikes  
- flash‑crash‑like events  
- API glitches  
- abnormal buy/sell pressure  

This pipeline automatically flags behavior that looks unusual or risky — something humans cannot do manually at high frequency.

---

## 🌍 Real‑World Uses

This system can be used for:

- **market surveillance**  
- **risk monitoring**  
- **trading strategy protection**  
- **data quality checks**  
- **research on volatility and anomalies**  

It demonstrates end‑to‑end skills in streaming, Docker, cloud data engineering, feature engineering, machine learning, and warehousing.

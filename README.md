# Real‑Time Crypto Anomaly Detection Pipeline

A fully containerized, cloud‑native data engineering and machine learning pipeline that ingests **live cryptocurrency trades**, processes them through a **Kafka streaming system**, cleans and enriches the data using **AWS Glue (Spark)**, detects anomalies using **unsupervised ML**, and loads the results into **Snowflake** for analytics.

This project mirrors real‑world architectures used in fintech, trading, and market‑surveillance systems.

---

# Architecture Diagram

> *(Insert your generated architecture image here)*  
> Example placeholder:  
> `![Architecture](images/architecture.png)`

---

# Tech Stack

### **Streaming & Ingestion**
- Kafka  
- WebSockets  
- Docker  

### **Cloud & Storage**
- AWS S3 (Raw, Clean, ML Zones)  
- AWS Glue (Spark ETL + ML)  

### **Machine Learning**
- PySpark MLlib  
- KMeans clustering  
- Feature engineering (time, categorical, numeric)  

### **Analytics**
- Snowflake  
- SQL  

### **Languages**
- Python  
- SQL  

---

# 1. Real‑Time Data Ingestion (Coinbase → Kafka)

A Python WebSocket client streams live crypto trades (BTC, ETH, SOL, DOGE, ADA) from Coinbase and publishes them to Kafka in real time.

**Why Kafka?**
- Handles high‑frequency market data  
- Provides durability and backpressure  
- Decouples ingestion from downstream processing  
- Mirrors real trading infrastructure  

All ingestion components run in Docker for reproducibility and clean networking.

---

# 2. RAW Data Lake Layer (S3)

A Kafka consumer batches messages and writes them to an **S3 RAW zone** as newline‑delimited JSON.

**Purpose of RAW zone**
- Immutable source of truth  
- Enables replay and reprocessing  
- Supports auditability and debugging  

This follows modern data lake best practices.

---

# 3. Glue Job #1 — Cleaning & Validation

A Spark‑based ETL job performs:

- schema enforcement  
- timestamp parsing  
- numeric casting  
- filtering invalid rows  
- validating symbols and buy/sell values  

**Why this matters:**  
Anomaly detection is extremely sensitive to noise. Clean data ensures the model learns real market behavior.

---

# 4. Glue Job #2 — Feature Engineering + Anomaly Detection

This job transforms each trade into a rich feature vector:

### **Feature Engineering**
- **Time features:** hour of day, day of week  
- **Categorical encodings:** symbol, buy/sell  
- **Numeric features:** price, volume  

### **Modeling**
A **KMeans clustering model** is trained to learn normal trading behavior.  
Anomaly scores are computed as the distance from each point to its cluster center.

### **Outputs**
- anomaly score  
- cluster assignment  
- binary anomaly flag (top 1% of scores)  
- saved Spark ML pipeline  
- scored Parquet dataset  

This creates a reusable ML model and a fully scored dataset for analytics.

---

# 5. Snowflake Analytics Layer

The scored Parquet files are loaded into Snowflake for:

- dashboards  
- anomaly monitoring  
- symbol‑level analysis  
- time‑of‑day anomaly patterns  
- joining with other datasets  

Example query:

```sql
SELECT *
FROM crypto_anomalies
WHERE anomaly_prediction = 1
ORDER BY anomaly_score DESC;

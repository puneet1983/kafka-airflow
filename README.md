
##   Real-Time Trade ETL Pipeline

A Docker-based, real-time ETL pipeline to ingest, validate, process, and store trade events for analytics, compliance, and reporting. Built using **Apache Kafka**, **Apache Airflow**, **DuckDB**, and **Python**.

---

###       Overview

This project simulates a real-world trade processing platform where thousands of trades are generated, validated against business rules, and persisted in an analytical store. The pipeline ensures **data quality**, **version control**, and **auditability**.

---

## Architecture

**High-Level Flow**

```
Trade Producer(simulated data in json format) → Kafka (raw_trades) → Consumer & Validator → DuckDB → Airflow Orchestration → Alerts
```

*(See `/docs/architecture.puml` for the full diagram.)*

---

##   Features

- Real-time ingestion using Apache Kafka  
- Schema validation & transformation  
- Business rule enforcement  
- Version-controlled trade processing  
- Orchestrated workflows with Airflow  
- Analytical storage using DuckDB  
- Docker-based local environment  
- Monitoring & alerting  

---

## Project Structure

```text
etl-trade-pipeline/
├── docker-compose.yml
├── airflow/
│   ├── dags/
│   
├── logs/
│   ├── DAG_1
│   └── DAG_2
├── scripts/
│   └── consumer_batch.py    
│   └── load_delta.py
│   └── schema.py
│   └── trade_generator.py
│   
├── db/
│   └── trades.db
├── storage/
│   └── raw
│         └── batch_<timestamp>.json
├── docs/
│   └── architecture.puml
└── README.md
```

---

## ⚙️ Prerequisites

- Docker Desktop  
- Docker Compose  
- Python 3.9+  
- Git  

---

## 🛠️ Setup & Installation

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/your-org/etl-trade-pipeline.git
cd etl-trade-pipeline
```

---

### 2️⃣ Start the Infrastructure
```bash
docker compose up -d
```

---

### 3️⃣ Initialize Airflow

```bash
docker compose run airflow-webserver airflow db init

docker compose run airflow-webserver airflow users create   --username admin   --password <pwd>   --firstname Puneet   --lastname Jaiswal   --role Admin   --email <email>
```

---

### 4️⃣ Initialize DuckDB

```bash
docker compose exec airflow-webserver python scripts/init_db.py
```

---

### 5️⃣ Start Trade Producer

- Open Airflow UI → http://localhost:8080  
- Enable DAG: `trade_simulation`  

---

### 6️⃣ Run the Pipeline

- Open Airflow UI → http://localhost:8080  
- Enable DAG: `trade_validation_pipeline`  

---

## 📊 Business Rules

| Rule | Action |
|------|--------|
| Incoming version < existing | Reject |
| Incoming version = existing | Replace |
| Maturity date < today | Reject |
| Maturity date passed | Mark as `EXPIRED` |

---

## 🗄️ Data Storage

**Database:** DuckDB (`db/trades.db`)

**Tables:**
- `valid_trades`  
- `invalid_trades`  
- `staging_valid_trades`
- `staging_rejected_trades`  

---

## 🔔 Monitoring & Alerts

- Airflow task retries & logs  
- Email alerts on task failure  

---

## 🔐 Security & Governance

- Role-based access to DB  
- Encrypted Kafka traffic (in prod)  
- Audit logging enabled  

---


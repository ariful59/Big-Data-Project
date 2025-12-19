
# Big Data Project

A distributed data processing and machine learning pipeline using **Apache Spark**, **Docker**, and **Python**.

---

## 📌 Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Setup Instructions](#setup-instructions)
5. [Execution Steps](#execution-steps)
6. [Project Structure](#project-structure)
7. [Features](#features)
8. [Monitoring (Spark UI & History Server)](#monitoring-spark-ui--history-server)
9. [Troubleshooting](#troubleshooting)
10. [License](#license)

---

## Overview
This project demonstrates:
- Distributed data ingestion across multiple Spark nodes
- Data unification and preprocessing
- Feature engineering
- Machine Learning pipeline with hyperparameter tuning
- Scalable execution using Dockerized Spark cluster

---

## Architecture
```
+-------------------+
|   Spark Master    |  <-- Cluster Manager
+-------------------+
        |
        |  spark://spark-master:7077
        |
+-------------------+    +-------------------+
| Spark Worker 1    |    | Spark Worker 2    |
+-------------------+    +-------------------+
```

---

## Prerequisites
- **Docker** & **Docker Compose**
- **Python 3.8+**
- **Git**

Install Docker: https://docs.docker.com/get-docker/

---

## Setup Instructions
```bash
# 1) Clone repository
git clone https://github.com/ariful59/Big-Data-Project.git
cd Big-Data-Project

# 2) Start Spark cluster (Master + 2 Workers)
docker-compose up --build -d

# 3) Run initial setup script (prepares folders, sample data, etc.)
python3 run_script.py
```

> After the cluster is up, containers available:
> - spark-master
> - spark-worker-1
> - spark-worker-2

---

## Execution Steps

### 1) Ingest Data on Each Node
Run ingestion with explicit environment variables per node.

#### Master Node
```bash
docker exec -it \
  --env NODE_ID=master \
  --env NODE_FILE=/master_data/adult_master.csv \
  --env OUT_BASE=/app/data \
  spark-master \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py
```

#### Worker 1
```bash
docker exec -it \
  --env NODE_ID=worker1 \
  --env NODE_FILE=/worker1_data/adult_worker1.csv \
  --env OUT_BASE=/app/data \
  spark-worker-1 \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py
```

#### Worker 2
```bash
docker exec -it \
  --env NODE_ID=worker2 \
  --env NODE_FILE=/worker2_data/adult_worker2.csv \
  --env OUT_BASE=/app/data \
  spark-worker-2 \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py
```

> These commands write node-local outputs into `/app/data/<NODE_ID>/...` inside each container.

---

### 2) Combine Data into Unified File (on Master)
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/unified_file.py
```
Creates a unified **Parquet** dataset gathering data produced by all nodes.

---

### 3) Data Preprocessing (Cleaning + Feature Engineering)
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/data_preprocessing.py
```
Outputs cleaned and feature-engineered data to:
```
/app/output/final_unified
```

---

### 4) ML Pipeline (Baseline)
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/ml_pipeline.py
```

---

### 5) ML Pipeline with Hyperparameter Tuning + Cross-Validation
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/ml_pipeline_with_hyper_paramater_and_cross_validation.py
```

---

### 6) Split Final Dataset
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/split_final_dataset.py
```

---

### 7) Run All Together (Part-2: Distributed ML from Each Node)
Launch the same ML job from **each container**, using cluster master.

#### Master Job
```bash
docker exec -it \
  --env NODE_ID=master \
  spark-master \
  spark-submit \
    --conf spark.cores.max=2 \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --master spark://spark-master:7077 \
    /app/ml_pipeline.py &
```

#### Worker 1 Job
```bash
docker exec -it \
  --env NODE_ID=worker1 \
  spark-worker-1 \
  spark-submit \
    --conf spark.cores.max=2 \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --conf spark.ui.port=4040 \
    --master spark://spark-master:7077 \
    /app/ml_pipeline.py &
```

#### Worker 2 Job
```bash
docker exec -it \
  --env NODE_ID=worker2 \
  spark-worker-2 \
  spark-submit \
    --conf spark.cores.max=2 \
    --conf spark.executor.instances=2 \
    --conf spark.executor.cores=2 \
    --conf spark.executor.memory=2g \
    --conf spark.ui.port=4041 \
    --master spark://spark-master:7077 \
    /app/ml_pipeline.py
```
> Note: Set different `spark.ui.port` per concurrent app (e.g., 4040, 4041) to avoid UI port conflicts.

---

### 8) Simultaneous Distributed ML on Unified Dataset (Master)
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/ml_pipeline.py
```

---

## Monitoring (Spark UI & History Server)

### Spark UI (per running app)
- Master: `http://localhost:8080` (cluster overview)
- Application UI (inside each job): `http://localhost:4040` (or the port you set)

### History Server (view past jobs)
Ensure event logs dir exists and start the server in master container:
```bash
docker exec -it spark-master bash -lc 'mkdir -p /tmp/spark-events && \
  $SPARK_HOME/sbin/start-history-server.sh'
```
If you want to enable event logging in jobs, add:
```bash
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
```
to your `spark-submit` commands.

Then open: `http://localhost:18080` for the History Server UI.

---

## Project Structure
```
Big-Data-Project/
│
├── docker-compose.yml
├── run_script.py
├── ingest_data.py
├── unified_file.py
├── data_preprocessing.py
├── ml_pipeline.py
├── ml_pipeline_with_hyper_paramater_and_cross_validation.py
├── split_final_dataset.py
└── README.md
```

---

## Features
- Distributed Spark cluster using Docker
- Data ingestion from multiple nodes
- Unified dataset creation in Parquet format
- Data cleaning & feature engineering
- ML pipeline with hyperparameter tuning & cross-validation
- Spark History Server and per-app UI monitoring

---

## Troubleshooting
- **Port conflicts**: Set unique `--conf spark.ui.port` for each concurrent job.
- **Container not found**: Ensure `docker-compose up --build -d` ran successfully.
- **File not found**: Verify data paths (`/master_data`, `/worker1_data`, `/worker2_data`) exist in containers; these are usually mounted via `docker-compose.yml`.
- **Insufficient resources**: Adjust `spark.executor.memory`, `spark.executor.cores`, and `spark.executor.instances`.

---

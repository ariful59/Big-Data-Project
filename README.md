
# Big Data Project – Setup & Execution Guide

## ✅ 1. Clone the Repository
```bash
git clone https://github.com/ariful59/Big-Data-Project.git
cd Big-Data-Project
```

---

## ✅ 2. Install Docker
Make sure Docker and Docker Compose are installed.  
[Install Docker](https://docs.docker.com/get-docker/) if not already installed.

---

## ✅ 3. Start Spark Cluster (Master + 2 Workers)
Build and start the cluster in detached mode:
```bash
docker-compose up --build -d
```

This will create:
- **spark-master**
- **spark-worker-1**
- **spark-worker-2**

---

## ✅ 4. Run Initial Setup Script
Prepare the environment:
```bash
python3 ./app/run_script.py
```

---

## ✅ 5. Ingest Data on Each Node
Run these commands to load data into Spark from each node:

### Master Node:
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

### Worker 1:
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

### Worker 2:
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

---

## ✅ 6. Combine Data into Unified File
Run the unification script on the master:
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/unified_file.py
```

This creates a unified Parquet dataset from all nodes.

---

## ✅ 7. Data Preprocessing (Cleaning + Feature Engineering)
Run the preprocessing script:
```bash
docker exec -it spark-master \
  spark-submit --master spark://spark-master:7077 /app/data_preprocessing.py
```

This step:
- Loads the unified dataset
- Cleans missing values and normalizes text
- Creates engineered features (e.g., income flag, age buckets)
- Repartitions for parallel processing
- Writes the final dataset to `/app/output/final_unified`

---



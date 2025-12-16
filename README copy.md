First clone the repo: https://github.com/ariful59/Big-Data-Project.git

Install Docker

Run command for creating master wtih two worker
docker-compose up --build -d


First run the run_script using python3 ./app/run_script.py (run normal python file)


Then run those command, 
docker exec -it \
  --env NODE_ID=master \
  --env NODE_FILE=/master_data/adult_master.csv \
  --env OUT_BASE=/app/data \
  spark-master \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py



docker exec -it \
  --env NODE_ID=worker1 \
  --env NODE_FILE=/worker1_data/adult_worker1.csv \
  --env OUT_BASE=/app/data \
  spark-worker-1 \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py 


docker exec -it \
  --env NODE_ID=worker2 \
  --env NODE_FILE=/worker2_data/adult_worker2.csv \
  --env OUT_BASE=/app/data \
  spark-worker-2 \
  spark-submit \
    --master 'local[*]' \
    /app/ingest_data.py
   


then 

docker exec -it spark-master spark-submit --master spark://spark-master:7077 /app/unified_file.py


then 

docker exec -it spark-master spark-submit --master spark://spark-master:7077 /app/data_preprocessing.py  



1) Local mode (--master local[*])
Everything (driver + tasks) runs inside the same process/container. No external master/workers are used.
┌────────────────────────────────────────────────────────────┐
│        Single container / process (local[*])               │
│                                                            │
│   spark-submit  ──►  Driver (your Python script)           │
│                                    │                       │
│                                    ▼                       │
│                              Local executor threads        │
│                              (use all cores: [*])          │
│                                                            │
│   Files read here are local to THIS container              │
└────────────────────────────────────────────────────────────┘


2) Standalone cluster, client deploy mode
(what you use with --master spark://spark-master:7077 by default)
The driver runs in the container where you execute spark-submit. The master coordinates and launches executors on workers.
Host (your shell)
    │
    ├─ docker exec spark-master spark-submit ...
    ▼
┌────────────────────────────────────────────────────────────┐
│                  spark-master (container)                  │
│                                                            │
│  Driver (your Python script)                               │
│      │                                                     │
│      │  connects to                                        │
│      ▼                                                     │
│  Master (cluster manager) ───────────────────────┐         │
└──────────────────────────────────────────────────┘         │
                                                             │
                                         schedules executors │
                                                             ▼
┌───────────────────────────────┐     ┌───────────────────────────────┐
│        spark-worker-1         │     │        spark-worker-2         │
│  Executors (JVM processes)    │     │  Executors (JVM processes)    │
│  run tasks on data            │     │  run tasks on data            │
└───────────────────────────────┘     └───────────────────────────────┘

**Important:** Executors read the input paths. Those paths must be
visible to workers (e.g., /app/shared), or you’ll get “file not exist”.

Use when: Data is in a shared path (e.g., /app/..., NFS, HDFS, S3) accessible to all workers.

3) Standalone cluster, cluster deploy mode
You submit the job to the master; the master launches the driver on one of the workers. Executors run (often on the same or other workers).
Host (your shell)
    │
    ├─ spark-submit --deploy-mode cluster ...
    ▼
┌───────────────────────────────┐
│           Master              │
│ (accepts app, chooses worker) │
└───────────────────────────────┘
                │
                │ launches driver on some worker
                ▼
┌───────────────────────────────┐
│   Chosen Worker (e.g., W2)    │
│   Driver (your Python script) │
│       │                       │
│       │ requests executors    │
│       ▼                       │
│   Executors (local + remote)  │  ← may run here and on other workers
└───────────────────────────────┘

**Important:** Any file paths the driver/executors read must be on
shared storage. Node‑local paths (e.g., /master_data) won’t work.

 

What is this dataset?

It contains 32,561 rows and 15 columns.
It’s widely known as the Adult Income dataset (from UCI ML repository).
Goal: Predict whether a person earns more than $50K/year based on demographic and work-related attributes.


✅ Target (Label)

income → Binary classification:

<=50K (low income)
>50K (high income)


This is the column you predict.


✅ Features (Inputs)
All other columns except income are candidate features:
Numeric Features

age → Age in years.
fnlwgt → Sampling weight (often ignored in ML unless weighting is needed).
education.num → Education level as number (1–16).
capital.gain → Investment gains.
capital.loss → Investment losses.
hours.per.week → Hours worked per week.

Categorical Features

workclass → Employment type (Private, Self-emp, etc.).
education → Education level (Bachelors, HS-grad, etc.).
marital.status → Marital status (Married, Divorced, etc.).
occupation → Job type (Exec-managerial, Sales, etc.).
relationship → Family relationship (Husband, Wife, etc.).
race → Race.
sex → Gender.
native.country → Country of origin.


✅ Missing Values

Represented by ? in categorical columns:

workclass: 1,836 missing
occupation: 1,843 missing
native.country: 583 missing




✅ Why these features matter?

Age, education, hours per week → Strong predictors of income.
Capital gain/loss → Highly skewed but very informative.
Workclass, occupation, marital status → Socio-economic indicators.
Race, sex, native country → Often included for fairness analysis.


✅ Typical ML Setup

Label: income (convert to 0/1: >50K → 1, <=50K → 0)
Features:

Numeric: age, education.num, hours.per.week, capital.gain, capital.loss
Categorical: workclass, education, marital.status, occupation, relationship, race, sex, native.country


Drop or ignore: fnlwgt (usually not predictive for income)
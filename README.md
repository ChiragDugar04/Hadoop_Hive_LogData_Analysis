# Log Data Analysis Pipeline with Hadoop, Hive, and Streamlit

## Project Overview

This project is a **Data engineering pipeline** that ingests web server log data, processes it, and allows analysis using both Hive and a Streamlit frontend. It demonstrates core data engineering skills, including:

- HDFS ingestion
- Hive table creation, partitioning, and bucketing
- Data transformation with HiveQL
- Interactive frontend using Streamlit
- Integration of HDFS, Hive, and Python

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Prerequisites](#prerequisites)
3. [Environment Setup](#environment-setup)
4. [Phase 1: Data Ingestion](#phase-1-data-ingestion)
5. [Phase 2: Hive Tables & ETL](#phase-2-hive-tables--etl)
6. [Phase 3: Hive Analysis](#phase-3-hive-analysis)
7. [Phase 4: Streamlit Frontend](#phase-4-streamlit-frontend)
8. [Commands Reference](#commands-reference)
9. [License](#license)

---

## Project Structure

log-data-pipeline/
├── data/                       # Sample datasets and uploaded log files
├── frontend/                   # Streamlit frontend app
│   └── app.py
├── hive_scripts/               # Hive table creation & data load scripts
│   ├── create_external_table.sql
│   ├── create_processed_table.sql
│   └── load_processed_table.sql
├── mapreduce/                  # Optional MapReduce code
│   ├── LogProcessor.java
│   └── LogProcessor.jar
├── .gitignore
└── README.md

---

## Prerequisites

-   **Ubuntu** (tested on 20.04+)
-   **Java 8**
-   **Hadoop 2.9.1**
-   **Hive 2.3.5**
-   **Python 3.8+**
-   Python packages: `streamlit`, `pandas`, `python-dateutil`, `pyhive`, `sqlalchemy`, `thrift`, `thrift-sasl`
-   VS Code or any preferred IDE for editing scripts

---

## Environment Setup

1.  Set environment variables by adding the following to your `~/.bashrc` file. Remember to replace paths if yours differ.

    ```bash
    export JAVA_HOME=/home/chirag/jdk-8u202-linux-x64/jdk1.8.0_202
    export HADOOP_HOME=/home/chirag/hadoop-2.9.1
    export HIVE_HOME=/home/chirag/apache-hive-2.3.5-bin
    export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH

    source ~/.bashrc
    ```

2.  Navigate to the project directory and set up the Python environment.

    ```bash
    cd ~/log-data-pipeline
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install streamlit pandas python-dateutil pyhive thrift sqlalchemy thrift-sasl
    ```

---

## Phase 1: Data Ingestion

1.  Place your log files in the local `data/` folder or download a sample dataset like the NASA HTTP dataset.

2.  Create a directory in HDFS and upload your log files.

    ```bash
    hadoop fs -mkdir -p /user/chirag/raw_logs
    hadoop fs -put ~/log-data-pipeline/data/<logfile> /user/chirag/raw_logs/
    ```

---

## Phase 2: Hive Tables & ETL

1.  Create an **external table** in Hive that points to the raw log files in HDFS. This step defines the schema without moving the data.

    ```bash
    hive
    > SOURCE ~/log-data-pipeline/hive_scripts/create_external_table.sql;
    ```

2.  Create a second, **managed table** that is optimized for analysis with partitioning and bucketing.

    ```bash
    hive
    > SOURCE ~/log-data-pipeline/hive_scripts/create_processed_table.sql;
    ```

3.  Load the data from the raw table into the processed table, applying transformations and leveraging dynamic partitioning.

    ```bash
    hive
    > SOURCE ~/log-data-pipeline/hive_scripts/load_processed_table.sql;
    ```

---

## Phase 3: Hive Analysis

Once the data is processed, you can run powerful analytical queries on the `processed_logs` table.

```sql
-- Example: Top 10 most requested URLs
SELECT request, COUNT(*) AS total_requests
FROM processed_logs
GROUP BY request
ORDER BY total_requests DESC
LIMIT 10;

---

## Phase 4: Streamlit Frontend

Run the interactive web application to analyze the data.

```bash
cd ~/log-data-pipeline
source venv/bin/activate
streamlit run frontend/app.py
```

---

## Commands Reference
Command	Description

hadoop fs -mkdir -p /user/chirag/raw_logs	       Creates a directory in HDFS.

hadoop fs -put <local_file> /user/chirag/raw_logs/	Uploads a file from local to HDFS.

hive -f hive_scripts/create_processed_table.sql	    Executes a Hive script.

streamlit run frontend/app.py	                  Runs the Streamlit web app.

---

## License
MIT License © 2025 Chirag Dugar

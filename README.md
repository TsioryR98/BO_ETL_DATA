# BO_ETL_DATA

An ETL pipeline orchestrated with Apache Airflow designed to consolidate Back-Office agent performance metrics—including
calls, emails, CSAT, evaluations, and nominations—for downstream BI analysis.

---

```
Raw Excel Files  →  Airflow Tasks (Extract + Transform)  →  Normalized Parquet / CSV
```

---

## ETL Pipeline

The Airflow DAG `production_etl_dag.py` handles the end-to-end orchestration.

> **Convention de nommage :** Naming Convention: All agent-related columns are normalized to lowercase and stripped of
> leading/trailing whitespace `(str.lower().str.strip())`. Dates are processed using standardized normalizers to ensure
> cross-source consistency.
>

---

## Installation

### Prerequisites

- Python 3.9+
- Apache Airflow 3.x

### Setup

```bash
git clone https://github.com/TsioryR98/BO_ETL_DATA.git
cd BO_ETL_DATA

python -m venv python_venv
source python_venv/bin/activate  

pip install -r requirements.txt

export AIRFLOW_HOME=$(pwd)
airflow db init
```

### Core Dependencies

```
apache-airflow
pandas
openpyxl
pyarrow
```

---

## Usage

1. Place your raw Excel source files into the `data/raw/` directory
2. Start the scheduler, webserver, and dag-processor in separate terminals (or as background tasks) :

```bash
airflow scheduler &
airflow webserver --port 8080 &
airflow dag-processor
```

3. enable the `production_etl_dag` with the Airflow UI at (`http://localhost:8080`)
4. Transformed and normalized files will be generated in `data/tmp/` and `data/processed/` directories for downstream
   analysis.

---

## Project Structure

```
.gitignore          
requirements.txt    
profiles.ipynb      
```

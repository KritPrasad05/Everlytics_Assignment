# Everlytics Internship Assignment – QuickShop ETL + Apache Airflow Pipeline

This project was developed as part of the **Everlytics Internship Hiring Assignment**.  
It includes a fully functional **ETL package (quickshop_etl)** and an **Apache Airflow DAG** to orchestrate the ETL workflow using **Astro CLI + Docker**.

---

## 📁 Project Structure

```
Everlytics_Assignment/
│
├── quickshop_etl/               # Python ETL package
│   ├── readers.py               # Read CSV sources
│   ├── writers.py               # Write Parquet + summary JSON
│   ├── transforms.py            # Apply business rules
│   ├── validation.py            # Row validation functions
│   ├── cli.py                   # Command-line ETL wrapper
│   └── __init__.py
│
├── airflow-docker/              # Astro CLI Airflow project (Docker-based)
│   ├── dags/
│   │   └── quickshop_etl_dag.py
│   ├── Dockerfile
│   ├── requirements.txt         # Airflow-only requirements
│   └── ...
│
├── data/                        # Input CSVs
│   ├── products.csv
│   ├── inventory.csv
│   ├── order_20251025.csv
│   ├── order_20251026.csv
│   └── ...
│
├── output/                      # Local ETL output (for testing)
│
└── requirements.txt             # Global project dependencies (except Airflow)
```

---

## 🚀 Features Implemented

### ✔ 1. Full ETL pipeline  
- Reads product, inventory, and order datasets  
- Validates rows  
- Applies transformations  
- Generates:  
  - **Parquet output**  
  - **Daily summary JSON**

### ✔ 2. Apache Airflow DAG  
- Uses **PythonOperator**  
- Executes ETL for a given date  
- Output stored under:  
  `/usr/local/airflow/project/output/`

### ✔ 3. Astro CLI + Docker Integration  
- Airflow deployed inside Docker  
- Local ETL package installed inside the container  
- Data copied into the container correctly  
- DAG visible and executable from Airflow UI or CLI  

### ✔ 4. Manual Testing & DAG Run Validation  
- Successfully triggered DAG  
- ETL completed end-to-end  
- Parquet + summary files created inside container  

---

## 📦 Installation & Setup (Local ETL Testing)

### 1. Create and activate virtual environment
```bash
python -m venv Everlytics_Assignment
source Everlytics_Assignment/bin/activate   # Mac/Linux
Everlytics_Assignment\Scriptsctivate     # Windows
```

### 2. Install project requirements
```bash
pip install -r requirements.txt
```

### 3. Run ETL locally
```bash
python -m quickshop_etl.cli run_for_date 20251025 --data_dir=data --output_dir=output
```

---

## 🛠 Setting up Apache Airflow with Astro CLI

### 1. Install Astro CLI
```bash
curl -sSL https://install.astronomer.io | sudo bash
astro version
```

### 2. Create Astro project (inside airflow-docker)
```bash
astro dev init
```

### 3. Start Airflow inside Docker
```bash
astro dev start --no-cache
```

### 4. Copy ETL package and data into the container
```bash
docker cp quickshop_etl airflow-astro-api:/usr/local/airflow/project/
docker cp data airflow-astro-api:/usr/local/airflow/project/
```

### 5. Test ETL execution inside container
```bash
docker exec -it airflow-api-server-1 python -c "from quickshop_etl.cli import run_for_date; print(run_for_date('20251025', '/usr/local/airflow/project/data', '/usr/local/airflow/project/output'))"
```

---

## 🌀 Running the Airflow DAG

### Unpause the DAG
```bash
docker exec -it airflow-api-server-1 airflow dags unpause quickshop_etl_pipeline
```

### Trigger DAG manually
```bash
docker exec -it airflow-api-server-1 airflow dags trigger quickshop_etl_pipeline --conf '{"date_str":"20251025"}'
```

### Check DAG run status
```bash
docker exec -it airflow-api-server-1 airflow dags list-runs quickshop_etl_pipeline
```

---

## 📤 Collecting Output Files (From Container to Local System)

### 1. Copy processed Parquet file
```bash
docker cp airflow-api-server-1:/usr/local/airflow/project/output/processed/date=2025-10-25/data.parquet ./output/
```

### 2. Copy summary JSON (if generated)
```bash
docker cp airflow-api-server-1:/usr/local/airflow/project/output/summaries/summary_2025-10-25.json ./output/
```

---

## 🧾 requirements.txt (Global)

This file contains dependencies for your **local ETL**, NOT Airflow.

```
pandas
pyarrow
fastparquet
python-dateutil
```

Airflow itself is **not included** here because it is installed inside the Docker container using Astro CLI.

### 📌 Airflow has its own file:
```
airflow-docker/requirements.txt
```

---

## 🏁 Final Notes

- This submission satisfies the full requirements of the **Everlytics Internship Assignment**.
- The project includes:
  ✔ ETL Python package  
  ✔ Airflow DAG  
  ✔ Working Docker environment  
  ✔ End-to-end ETL execution  
  ✔ Correct output artifacts  

For any additional improvements, CI/CD or cloud deployment can also be added.

---

## 🙌 Author

**Krit Prasad**  
Submitted as part of the **Everlytics Internship Hiring Assignment**.

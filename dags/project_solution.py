"""
Частина 2. Етап 4: Airflow DAG
Послідовно запускає всі три Spark-jobs через python:
  1. landing_to_bronze.py
  2. bronze_to_silver.py
  3. silver_to_gold.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Шлях до папки з Python-скриптами у контейнері
SCRIPTS_DIR = "/opt/airflow/dags/scripts"

default_args = {
    "owner": "de_student",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="goit_de_fp_pipeline",
    description="Фінальний проєкт: End-to-End Batch Data Lake (Landing → Bronze → Silver → Gold)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["goit", "de", "final_project", "batch"],
) as dag:

    # Етап 1: Завантаження даних з FTP та запис у Bronze у форматі Parquet
    landing_to_bronze = BashOperator(
        task_id="landing_to_bronze",
        bash_command=f"python {SCRIPTS_DIR}/landing_to_bronze.py",
        doc_md="""
        **Етап 1 — Landing → Bronze**
        Завантажує CSV з FTP-сервера та зберігає як Parquet у папку `bronze/`.
        """,
    )

    # Етап 2: Очищення тексту та дедублікація, запис у Silver
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"python {SCRIPTS_DIR}/bronze_to_silver.py",
        doc_md="""
        **Етап 2 — Bronze → Silver**
        Зчитує Bronze, чистить текстові поля, дедублікує, записує у `silver/`.
        """,
    )

    # Етап 3: Join + агрегація + timestamp, запис у Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"python {SCRIPTS_DIR}/silver_to_gold.py",
        doc_md="""
        **Етап 3 — Silver → Gold**
        Join таблиць, агрегація avg(height/weight) по sport/medal/sex/country_noc,
        додає timestamp, запизує у `gold/avg_stats`.
        """,
    )

    # Послідовний порядок виконання: 1 → 2 → 3
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
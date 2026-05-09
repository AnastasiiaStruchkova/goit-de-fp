"""
Частина 2. Етап 1: Landing to Bronze
Завантажує CSV-файли з FTP-сервера та зберігає їх у форматі Parquet у папку bronze/
"""

import requests
from pyspark.sql import SparkSession
import os

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("landing_to_bronze") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Таблиці для завантаження
TABLES = ["athlete_bio", "athlete_event_results"]
FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
LANDING_DIR = "/opt/airflow/data/landing"
BRONZE_DIR = "/opt/airflow/data/bronze"


def download_csv(table_name: str) -> str:
    """Завантажує CSV-файл з FTP-сервера у локальну папку landing/"""
    os.makedirs(LANDING_DIR, exist_ok=True)
    url = f"{FTP_BASE_URL}{table_name}.csv"
    local_path = f"{LANDING_DIR}/{table_name}.csv"

    print(f"[Етап 1] Завантаження файлу: {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    print(f"[Етап 1] Файл збережено: {local_path} ({len(response.content)} bytes)")
    return local_path


def csv_to_bronze(table_name: str, csv_path: str):
    """Зчитує CSV за допомогою Spark і зберігає у форматі Parquet у bronze/"""
    output_path = f"{BRONZE_DIR}/{table_name}"

    print(f"[Етап 1] Зчитування CSV: {csv_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    print(f"[Етап 1] Кількість рядків у {table_name}: {df.count()}")
    df.show(5, truncate=False)

    print(f"[Етап 1] Запис у bronze: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"[Етап 1] ✅ Таблицю {table_name} збережено у {output_path}")


# Головна логіка
for table in TABLES:
    csv_path = download_csv(table)
    csv_to_bronze(table, csv_path)

print("[Етап 1] ✅ Landing → Bronze завершено для всіх таблиць.")
spark.stop()

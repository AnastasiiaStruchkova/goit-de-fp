"""
Частина 2. Етап 2: Bronze to Silver
Зчитує Parquet із bronze/, очищає текстові поля, дедублікує рядки,
записує результат у silver/
"""

import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("bronze_to_silver") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

TABLES = ["athlete_bio", "athlete_event_results"]
BRONZE_DIR = "/opt/airflow/data/bronze"
SILVER_DIR = "/opt/airflow/data/silver"


def clean_text_udf(value):
    """UDF: очищає текстові поля — прибирає зайві пробіли та спеціальні символи"""
    if value is None:
        return None
    # Видаляємо всі символи крім літер, цифр, пробілів та базової пунктуації
    cleaned = re.sub(r"[^\w\s,.\-/]", "", value, flags=re.UNICODE)
    return cleaned.strip()


clean_udf = F.udf(clean_text_udf, StringType())


def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    [Етап 2] Чистка тексту для всіх текстових (string) колонок DataFrame
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    print(f"[Етап 2] Текстові колонки для очищення: {string_cols}")

    for col_name in string_cols:
        df = df.withColumn(col_name, clean_udf(F.col(col_name)))

    return df


def bronze_to_silver(table_name: str):
    bronze_path = f"{BRONZE_DIR}/{table_name}"
    silver_path = f"{SILVER_DIR}/{table_name}"

    # Зчитування bronze-таблиці
    print(f"[Етап 2] Зчитування bronze: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    print(f"[Етап 2] Рядків до обробки: {df.count()}")

    # Очищення текстових колонок
    df = clean_dataframe(df)

    # Дедублікація рядків
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    print(f"[Етап 2] Дедублікація: {before} → {after} рядків (видалено {before - after} дублікатів)")

    # Виведення зразка результату
    df.show(5, truncate=False)

    # Запис у silver
    print(f"[Етап 2] Запис у silver: {silver_path}")
    df.write.mode("overwrite").parquet(silver_path)
    print(f"[Етап 2] ✅ Таблицю {table_name} збережено у {silver_path}")


# Головна логіка
for table in TABLES:
    bronze_to_silver(table)

print("[Етап 2] ✅ Bronze → Silver завершено для всіх таблиць.")
spark.stop()

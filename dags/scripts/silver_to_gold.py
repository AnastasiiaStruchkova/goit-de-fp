"""
Частина 2. Етап 3: Silver to Gold
Зчитує silver-таблиці, робить join, обчислює середній зріст/вагу
по комбінації sport/medal/sex/country_noc, додає timestamp, зберігає у gold/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Ініціалізація Spark-сесії
spark = SparkSession.builder \
    .appName("silver_to_gold") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER_DIR = "/opt/airflow/data/silver"
GOLD_DIR = "/opt/airflow/data/gold"


# Зчитування silver-таблиць
print("[Етап 3] Зчитування silver/athlete_bio")
athlete_bio = spark.read.parquet(f"{SILVER_DIR}/athlete_bio")

print("[Етап 3] Зчитування silver/athlete_event_results")
athlete_events = spark.read.parquet(f"{SILVER_DIR}/athlete_event_results")

# Фільтруємо некоректні значення height та weight
athlete_bio = athlete_bio \
    .withColumn("height", F.col("height").cast("double")) \
    .withColumn("weight", F.col("weight").cast("double")) \
    .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())

print(f"[Етап 3] athlete_bio після фільтрації: {athlete_bio.count()} рядків")

# Join за колонкою athlete_id. 
# КЛЮЧОВЕ ВИПРАВЛЕННЯ: Видаляємо дубльований стовпець country_noc з athlete_bio
print("[Етап 3] Join athlete_events з athlete_bio за athlete_id")
joined = athlete_events.join(athlete_bio, on="athlete_id", how="inner") \
    .drop(athlete_bio.country_noc)

print(f"[Етап 3] Рядків після join: {joined.count()}")

# Обчислення середніх значень height і weight по sport, medal, sex, country_noc
print("[Етап 3] Агрегація: середній зріст і вага по sport/medal/sex/country_noc")
gold_df = joined.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        F.round(F.avg("height"), 2).alias("avg_height"),
        F.round(F.avg("weight"), 2).alias("avg_weight")
    )

# Додавання колонки timestamp із поточним часом виконання
gold_df = gold_df.withColumn("timestamp", F.current_timestamp())

print("[Етап 3] Зразок результату (gold/avg_stats):")
gold_df.show(20, truncate=False)

# Запис у gold/avg_stats
output_path = f"{GOLD_DIR}/avg_stats"
print(f"[Етап 3] Запис у {output_path}")
gold_df.write.mode("overwrite").parquet(output_path)

print(f"[Етап 3] ✅ Дані збережено у {output_path}")
spark.stop()
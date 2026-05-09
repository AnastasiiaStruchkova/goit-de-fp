from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"
MYSQL_JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"

KAFKA_BOOTSTRAP = "77.81.230.104:9092"
KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"
KAFKA_INPUT_TOPIC = "athlete_event_results"
KAFKA_OUTPUT_TOPIC = "athlete_event_results_out"

OUTPUT_MYSQL_TABLE = "olympic_dataset.avg_stats"

KAFKA_STREAM_OPTIONS = {
    "kafka.bootstrap.servers": KAFKA_BOOTSTRAP,
    "kafka.security.protocol": "SASL_PLAINTEXT",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
    ),
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "kafka.connections.max.idle.ms": "60000",
}

spark = SparkSession.builder \
    .appName("olympic_streaming_pipeline") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Етап 1: Зчитування athlete_bio з MySQL
print("[Етап 1] Зчитування athlete_bio...")
athlete_bio = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_JDBC_URL) \
    .option("dbtable", "athlete_bio") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

# Етап 2: Фільтрація — прибираємо порожні/нечислові height і weight
print("[Етап 2] Очищення даних athlete_bio...")
athlete_bio_clean = athlete_bio \
    .withColumn("height", F.expr("TRY_CAST(regexp_replace(height, ',', '') AS DOUBLE)")) \
    .withColumn("weight", F.expr("TRY_CAST(regexp_replace(weight, ',', '') AS DOUBLE)")) \
    .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())

athlete_bio_small = athlete_bio_clean.select(
    "athlete_id",
    F.col("sex").alias("sex"),
    F.col("height").alias("height"),
    F.col("weight").alias("weight")
)

# Етап 3: Зчитування athlete_event_results з MySQL і запис у Kafka
print("[Етап 3] Зчитування athlete_event_results з MySQL...")
athlete_events_mysql = spark.read \
    .format("jdbc") \
    .option("url", MYSQL_JDBC_URL) \
    .option("dbtable", "athlete_event_results") \
    .option("user", MYSQL_USER) \
    .option("password", MYSQL_PASSWORD) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

print("[Етап 3] Запис athlete_event_results у Kafka-топік...")
athlete_events_mysql \
    .select(F.to_json(F.struct("*")).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";') \
    .option("kafka.max.block.ms", "60000") \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.delivery.timeout.ms", "120000") \
    .option("kafka.linger.ms", "0") \
    .option("kafka.acks", "1") \
    .option("topic", KAFKA_INPUT_TOPIC) \
    .save()
print("[Етап 3] Запис у Kafka завершено.")

# Етап 3: Зчитування стріму з Kafka
print("[Етап 3] Зчитування стріму з Kafka...")
event_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("isTeamSport", StringType(), True),
])

events_stream = spark.readStream \
    .format("kafka") \
    .options(**KAFKA_STREAM_OPTIONS) \
    .option("subscribe", KAFKA_INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "10000") \
    .load()

events_parsed = events_stream \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(F.from_json(F.col("json_str"), event_schema).alias("data")) \
    .select("data.*")

# Етап 4: Join стріму з біо-даними
print("[Етап 4] Join стріму з athlete_bio...")
joined_stream = events_parsed.join(
    F.broadcast(athlete_bio_small),
    on="athlete_id",
    how="inner"
).select(
    events_parsed["athlete_id"],
    events_parsed["sport"],
    events_parsed["medal"],
    events_parsed["country_noc"],
    athlete_bio_small["sex"],
    athlete_bio_small["height"],
    athlete_bio_small["weight"]
)

# Етап 5: Агрегація — середній зріст і вага
print("[Етап 5] Агрегація...")
aggregated_stream = joined_stream \
    .groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        F.round(F.avg("height"), 2).alias("avg_height"),
        F.round(F.avg("weight"), 2).alias("avg_weight")
    ) \
    .withColumn("timestamp", F.current_timestamp())


# Етап 6: forEachBatch — запис у Kafka і MySQL
def foreach_batch_handler(batch_df: DataFrame, batch_id: int):
    print(f"\n[forEachBatch] Batch {batch_id}, рядків: {batch_df.count()}")
    batch_df.show(5, truncate=False)

    # Етап 6а: запис у вихідний Kafka-топік
    batch_df.select(F.to_json(F.struct("*")).alias("value")) \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";') \
        .option("kafka.max.block.ms", "60000") \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.delivery.timeout.ms", "120000") \
        .option("kafka.linger.ms", "0") \
        .option("kafka.acks", "1") \
        .option("topic", KAFKA_OUTPUT_TOPIC) \
        .save()
    print(f"[Етап 6а] Batch {batch_id} записано у Kafka.")

    # Етап 6б: запис у базу даних MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", MYSQL_JDBC_URL) \
        .option("dbtable", OUTPUT_MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
    print(f"[Етап 6б] Batch {batch_id} записано у MySQL.")


query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_handler) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/olympic_pipeline") \
    .trigger(processingTime="30 seconds") \
    .start()

print("[Стрім] Очікування завершення...")
query.awaitTermination()
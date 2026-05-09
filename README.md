# goit-de-fp — Фінальний проєкт з Data Engineering

## Частина 1. Streaming Pipeline

Стримінговий пайплайн на Apache Spark + Kafka + MySQL.

### Файли
- `part1/streaming_pipeline.py` — основний скрипт

### Етапи
1. Зчитування `athlete_bio` з MySQL
2. Фільтрація порожніх/нечислових height і weight
3. Запис `athlete_event_results` у Kafka-топік `athlete_event_results`, зчитування стріму
4. Join стріму з біо-даними за `athlete_id`
5. Агрегація: середній зріст і вага по `sport`, `medal`, `sex`, `country_noc`
6. Запис результатів у Kafka-топік `athlete_event_results_out` і MySQL таблицю `avg_stats`

### Запуск
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.mysql:mysql-connector-j:8.0.33 \
  part1/streaming_pipeline.py
```

---

## Частина 2. Batch Data Lake (Landing → Bronze → Silver → Gold)

Multi-hop datalake на Apache Spark + Airflow.

### Файли
- `dags/project_solution.py` — Airflow DAG
- `dags/scripts/landing_to_bronze.py` — завантаження CSV з FTP, запис у Parquet
- `dags/scripts/bronze_to_silver.py` — очищення тексту, дедублікація
- `dags/scripts/silver_to_gold.py` — join, агрегація, timestamp

### Запуск
```bash
docker-compose up -d
```
Відкрити Airflow: http://localhost:8080 (airflow/airflow)
Запустити DAG: `goit_de_fp_pipeline`

---

## Скріншоти результатів
Знаходяться у папці `sc/`

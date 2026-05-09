     1	# goit-de-fp — Фінальний проєкт з Data Engineering
     2	
     3	## Частина 1. Streaming Pipeline
     4	
     5	Стримінговий пайплайн на Apache Spark + Kafka + MySQL.
     6	
     7	### Файли
     8	- `part1/streaming_pipeline.py` — основний скрипт
     9	
    10	### Етапи
    11	1. Зчитування `athlete_bio` з MySQL
    12	2. Фільтрація порожніх/нечислових height і weight
    13	3. Запис `athlete_event_results` у Kafka-топік `athlete_event_results`, зчитування стріму
    14	4. Join стріму з біо-даними за `athlete_id`
    15	5. Агрегація: середній зріст і вага по `sport`, `medal`, `sex`, `country_noc`
    16	6. Запис результатів у Kafka-топік `athlete_event_results_out` і MySQL таблицю `avg_stats`
    17	
    18	### Запуск
    19	```bash
    20	spark-submit \
    21	  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,com.mysql:mysql-connector-j:8.0.33 \
    22	  part1/streaming_pipeline.py
    23	```
    24	
    25	---
    26	
    27	## Частина 2. Batch Data Lake (Landing → Bronze → Silver → Gold)
    28	
    29	Multi-hop datalake на Apache Spark + Airflow.
    30	
    31	### Файли
    32	- `dags/project_solution.py` — Airflow DAG
    33	- `dags/scripts/landing_to_bronze.py` — завантаження CSV з FTP, запис у Parquet
    34	- `dags/scripts/bronze_to_silver.py` — очищення тексту, дедублікація
    35	- `dags/scripts/silver_to_gold.py` — join, агрегація, timestamp
    36	
    37	### Запуск
    38	```bash
    39	docker-compose up -d
    40	```
    41	Відкрити Airflow: http://localhost:8080 (airflow/airflow)
    42	Запустити DAG: `goit_de_fp_pipeline`
    43	
    44	---
    45	
    46	## Скріншоти результатів
    47	Знаходяться у папці `sc/`

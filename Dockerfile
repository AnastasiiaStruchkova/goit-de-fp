FROM apache/airflow:2.9.1

USER root
# Встановлюємо Java, необхідну для роботи Spark
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Встановлюємо PySpark
RUN pip install --no-cache-dir pyspark==3.5.0 requests

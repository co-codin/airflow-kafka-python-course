# Course image: official Airflow 3 plus Kafka, DuckDB, and Faker.
# Rebuild after changing requirements.txt:
#   docker compose build
FROM apache/airflow:3.3.1

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

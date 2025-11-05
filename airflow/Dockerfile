FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y postgresql-client && apt-get install -y \
    gcc \
    # g++ \
    libpq-dev \    
    && rm -rf /var/lib/apt/lists/*


ENV AIRFLOW__CORE__EXECUTOR=SequentialExecutor
RUN pip install --upgrade pip
COPY requirements.txt .
RUN  pip install --no-cache-dir --timeout=100 --retries=5 \    
    -r requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.11.txt"


COPY config /app/airflow/dags/config
COPY dags/etl.py /app/airflow/dags
COPY migration.py /app/migration.py
COPY ./seed ./seed
COPY start-airflow.sh /app/start-airflow.sh
RUN chmod +x /app/start-airflow.sh

EXPOSE 9000

CMD ["/app/start-airflow.sh"]
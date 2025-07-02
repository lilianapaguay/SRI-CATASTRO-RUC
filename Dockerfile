FROM apache/airflow:3.0.1

# Install Postgres provider, psycopg2, HuggingFace SDK
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
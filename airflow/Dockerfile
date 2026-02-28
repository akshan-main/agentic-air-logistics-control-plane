FROM apache/airflow:2.9.3

# Install Snowflake + utility packages needed by the DAG
RUN pip install --no-cache-dir \
    snowflake-connector-python \
    snowflake-snowpark-python \
    snowflake \
    requests \
    psycopg2-binary

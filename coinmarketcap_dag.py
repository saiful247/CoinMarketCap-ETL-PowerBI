from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from coinmarketcap_etl import run_coinmarket_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 16),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'coinmarketcap_dag',
    default_args=default_args,
    description='My first DAG with ETL process!',
    schedule_interval=timedelta(hours=2),  # Run every 2 hours
)

run_etl = PythonOperator(
    task_id='complete_coinmarketcap_etl',
    python_callable=run_coinmarket_etl,
    dag=dag,
)

run_etl

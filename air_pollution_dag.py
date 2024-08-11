from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.collect_air_data import collect_air_data
from scripts.process_data import process_data
from scripts.analyze_data import analyze_data

# DAG definition and configuration here
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'air_pollution_analysis',
    default_args=default_args,
    description='A DAG for analyzing air pollution data',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Task definitions
collect_data_task = PythonOperator(
    task_id='collect_air_data',
    python_callable=collect_air_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

# Task dependencies
collect_data_task >> process_data_task >> analyze_data_task


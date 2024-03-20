import pendulum
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

sys.path.append(parent_dir)

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from main import execute

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024,2,26, tz="UTC"),
    'email': 'vitor@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="report__analytics_risk_analysis_obt_spotify_songs",
    description="DAG responsible for execute the job report",
    default_args=default_args,
    schedule= "0 6 * * 0",
    catchup=False,
    tags=["example"],
) as dag:

    start_task = PythonOperator(
        task_id="job_processing_to_analytics", 
        python_callable=execute, 
        dag=dag,
    )

    start_task
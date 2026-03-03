from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1)
}

with DAG('patient_etl_pipeline',
         schedule_interval='@once',
         default_args=default_args,
         catchup=False) as dag:

    start_producer = BashOperator(
        task_id='start_producer',
        bash_command='python /path/producer.py'
    )

    start_spark = BashOperator(
        task_id='start_spark',
        bash_command='spark-submit /path/spark_consumer.py'
    )

    start_flask = BashOperator(
        task_id='start_flask',
        bash_command='python /path/app.py'
    )

    start_producer >> start_spark >> start_flask
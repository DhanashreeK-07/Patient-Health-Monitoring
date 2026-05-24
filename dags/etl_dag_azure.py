from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Databricks cluster and job configuration
spark_job_json = {
    'new_cluster': {
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'Standard_D3_v2',
        'num_workers': 2
    },
    'spark_python_task': {
        # Assuming you linked your GitHub repo to Databricks Repos
        'python_file': '/Workspace/Repos/your_username/patient-health-monitor/src/spark_consumer_azure.py'
    }
}

with DAG(
    'patient_etl_azure_pipeline',
    default_args=default_args,
    description='Azure IoT Health Monitoring Pipeline',
    schedule_interval='@once',
    catchup=False
) as dag:

    # Task 1: Start streaming data via the Producer
    # Ensure the path matches where Airflow is running
    start_producer = BashOperator(
        task_id='start_eventhub_producer',
        bash_command='python /path/to/your/repo/patient-health-monitor/src/producer_azure.py'
    )

    # Task 2: Trigger Spark processing on Azure Databricks
    start_spark_databricks = DatabricksSubmitRunOperator(
        task_id='run_databricks_spark_job',
        databricks_conn_id='databricks_default', # Configure this connection in Airflow UI
        json=spark_job_json
    )
    
    # Define dependency
    start_producer >> start_spark_databricks

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':0
#    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_master_data_consumer_riverflow',
    default_args=default_args,
    description='Run Python script in Conda environment using BashOperator',
    schedule_interval= None,  # Run every 5 minutes,
    catchup=False
)

conda_env_path = os.environ['DE_CONDA_ENV']
file_name =  'AvroRiverFlowMasterDataConsumer.py'
file_path = os.environ['DE_KAFKA_FILE_PATH']

run_script_task = BashOperator(
    task_id='kafka_master_data_consumer_riverflow',
    bash_command=conda_env_path+' '+file_path+file_name,
    dag=dag
)

run_script_task

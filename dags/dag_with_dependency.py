from datetime import datetime, timedelta
import os
import sys

# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'richy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='dag_with_postgresv03',
        default_args=default_args,
        schedule='0 0 * * *',
        start_date=datetime(year=2023, month=7, day=5),
        catchup=False
    ) as dag:

    task1 = S3KeySensor(
        task_id='get_s3_id',
        bucket_name='minikubebucket',
        bucket_key='iris-dataset.csv',
        aws_conn_id='aws_conn',
        mode='poke',
        poke_interval=5,
        timeout=30
    )

    task1
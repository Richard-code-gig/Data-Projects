from datetime import datetime, timedelta
from os import path
from sys import path as sys_path

# sys_path.insert(0,path.abspath(path.dirname(__file__)))

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'richy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='dag_to_test_aws_connv01',
        default_args=default_args,
        schedule='0 0 * * *',
        start_date=datetime(year=2023, month=7, day=5),
        catchup=False
    ) as dag:

    test_s3_conn = S3KeySensor(
        task_id='get_s3_id',
        bucket_name='minikubebucket',
        bucket_key='return.csv',
        aws_conn_id='aws_conn',
        mode='poke',
        poke_interval=3,
        timeout=30
    )

test_s3_conn
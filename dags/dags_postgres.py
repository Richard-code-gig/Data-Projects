from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'richy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='dag_with_postgresv05',
         default_args=default_args,
        schedule='0 0 * * *',
        start_date=datetime(year=2023, month=7, day=5),
        catchup=False
        ) as dag:
    task1 = PostgresOperator(
        task_id='postgres_table_id',
        postgres_conn_id='postgres_conn_host',
        sql='''create table if not exists dag_runs (
        dt date,
        dag_id character varying,
        primary key (dt, dag_id)
        )'''
    )
    task2 = PostgresOperator(
        task_id='postgres_insert',
        postgres_conn_id='postgres_conn_host',
        sql='''insert into dag_runs(dt,dag_id)
        values('{{ ds }}', '{{ dag.dag_id }}')'''
    )
    task3 = PostgresOperator(
        task_id='postgres_delete',
        postgres_conn_id='postgres_conn_host',
        sql="""delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}'"""
    )
    task1 >> task3 >> task2
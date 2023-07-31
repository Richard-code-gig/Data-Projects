from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from stack_question import load_queston_to_postgres, spark_session
from stack_response import write_partition

def process_survey_data(*args, **kwargs):
    table_name = "public.survey_responses" 
    write_partition('/opt/airflow/data/survey_bucket', 'survey_results_public.csv', table_name, spark_session)
    spark_session.stop

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'task_timeout': timedelta(minutes=30),
}

dag = DAG(
    'load_postgres_script_dag',
    default_args=default_args,
    description='DAG to load_data.py script',
    schedule='@daily',
    catchup=False
)

run_question_task = PythonOperator(
    task_id='run_question_py',
    python_callable=load_queston_to_postgres,
    op_args=['/opt/airflow/data/survey_bucket', 'survey_results_schema.csv',
            'load_question_to_postgres', spark_session],
    dag=dag,
)

run_response_task = PythonOperator(
    task_id='run_response_py',
    python_callable=process_survey_data,
    provide_context=True, 
    dag=dag,
)

run_question_task >> run_response_task

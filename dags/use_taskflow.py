from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'richy',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(dag_id='dag_with_task_flow_v2',
     default_args=default_args,
     start_date=datetime(2023, 7, 22),
     schedule_interval='@daily')
def helloworld():
    
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Richard',
            'last_name': 'Cypher'
        }
    
    @task()
    def get_age():
        return 22
    
    @task()
    def greet(first_name, last_name, age):
        print(f'''My first name is {first_name}, 
              my last name is {last_name} and my age is {age}''')
    
    name_dic = get_name()
    age = get_age()
    greet(name_dic['first_name'], name_dic['last_name'], age)

greet_dag = helloworld()

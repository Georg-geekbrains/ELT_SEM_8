from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def print_hello():
    return 'Hello Airflow DAG!'

print(print_hello())
dag = DAG('my_first_dag', description='Hello World DAG',
          schedule='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

hello_operator
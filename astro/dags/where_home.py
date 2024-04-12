from airflow import DAG
from airflow.settings import AIRFLOW_HOME
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='where_is_home',
    start_date=datetime.now()
)

def command():

    print("$AIRFLOW_HOME=", AIRFLOW_HOME)

with dag:
    po = PythonOperator(
        task_id='print',
        python_callable=command,
        provide_context=True
    )

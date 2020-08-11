from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['thomas.wawra@avodi.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2020, 8, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sync_shows',
    default_args=default_args,
    description='sync shows',
    schedule_interval='*/10 0-23 * * *'
)

t1 = BashOperator(
    task_id='sync_shows',
    bash_command='/home/thomas/projects/dags/run_rscripts.sh /home/thomas/projects/bi/tasks__avodi__allianzcinema__gbq.R',
    dag=dag
)

t1
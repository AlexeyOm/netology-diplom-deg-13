from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 5),
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'email': ['alert@sample.com'],
    'email_on_failure': True,
}

dag = DAG('download_file', default_args=default_args, catchup=False, schedule_interval=None)

t1 = BashOperator(
    task_id='download_file',
    bash_command='curl -o /home/airflow/gcs/data/sample.json www.sample.com/api',
    dag=dag,
)

def run_script():
    import os
    os.chdir('/opt/airflow/dags')
    os.system('python scpt.py')

t2 = PythonOperator(
    task_id='run_script',
    python_callable=run_script,
    dag=dag,
)

t3 = EmailOperator(
    task_id='send_email',
    to=['alert@sample.com'],
    subject='Download File Failed',
    html_content="""<h3>The download file task has failed after 10 retries.</h3>""",
    dag=dag,
)

t1 >> t2
t1 >> t3
t2 >> t3
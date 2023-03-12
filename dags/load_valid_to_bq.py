from datetime import timedelta
from datetime import date

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import bigquery
from google.cloud import storage

from load_valid_to_bq_callable import get_good_rows_load_to_bq


default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 0,
    "retry_delay": timedelta(days=1),
}

dag = DAG(
    "load_valid_to_BQ",
    default_args=default_args,
    description="check last validation result, load valid rows to BQ",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=5),
)


today = date.today().strftime("%Y-%m-%d")
#загрузка данных по продажам по API
download_from_mockaroo = BashOperator(
    task_id="download_from_mockaroo",
    bash_command=f"curl -o  /home/airflow/gcs/data/supermarket_sales.csv -L 'https://my.api.mockaroo.com/mock_sales_data.csv?key=78343830&date={today}' ",  # put space in between single quote and double quote 
    dag=dag,
)


# проверка данных с помощью Great Expectation, результаты складываются в json в хранилище, определено как validation storage в
# great_expectations.yml
checkpoint_run = BashOperator(
    task_id="checkpoint_run",
    bash_command="(cd /home/airflow/gcsfuse/actual_mount_path/great_expectations/; great_expectations --v3-api checkpoint run production_checkpoint ) ",
    dag=dag,
    depends_on_past=False,
    cwd=dag.folder,
)


load_valid_to_bq = PythonOperator(
    task_id="load_to_bq",
    python_callable=get_good_rows_load_to_bq,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_done',
)


download_from_mockaroo >> checkpoint_run >> load_valid_to_bq
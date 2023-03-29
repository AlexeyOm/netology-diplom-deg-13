from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.email_operator import EmailOperator

from load_valid_to_bq_callable import get_good_rows_load_to_bq


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 21),
    'retries': 0
}


dag = DAG('etl_pipeline_8', start_date=datetime(2023, 3, 21, 6, 0), default_args=default_args, schedule_interval=timedelta(days=1))


#загрузка данных по продажам по API
download_from_mockaroo = BashOperator(
    task_id="download_from_mockaroo",
    bash_command="curl -o  /home/airflow/gcs/data/supermarket_sales.csv -L 'https://my.api.mockaroo.com/mock_sales_data.csv?key=78343830&date={{ ds }}' ",  # put space in between single quote and double quote 
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

# загрузка сырых данных в BQ
load_valid_to_bq = PythonOperator(
    task_id="load_to_bq",
    python_callable=get_good_rows_load_to_bq,
    dag=dag,
    depends_on_past=False,
    trigger_rule='all_done',
)


#обновление таблиц-справочников и измерений 
sales_branches = BigQueryOperator(
    task_id='sales_branches',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.branches","sales.dim_branches", "branch_id", "branch_name", "Branch", "{{ ds }}" )',
    dag=dag
)

#обновление таблиц-справочников и измерений
sales_cities = BigQueryOperator(
    task_id='sales_cities',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.cities","sales.dim_cities", "city_id", "city_name", "City", "{{ ds }}" )',
    dag=dag
)

#обновление таблиц-справочников и измерений
sales_product_lines = BigQueryOperator(
    task_id='sales_product_lines',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.product_lines","sales.dim_product_lines", "product_line_id", "product_line_name", "Product_line", "{{ ds }}" )',
    dag=dag
)

#обновление таблиц-справочников и измерений
sales_payment_types = BigQueryOperator(
    task_id='sales_payment_types',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.payment_types","sales.dim_payment_types", "payment_type_id", "payment_type_name", "Payment", "{{ ds }}" )',
    dag=dag
)

#обновление таблиц-справочников и измерений
sales_member_statuses = BigQueryOperator(
    task_id='sales_member_statuses',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.member_statuses","sales.dim_member_statuses", "member_status_id", "member_status_name", "Customer_type", "{{ ds }}" )',
    dag=dag
)

#обновление таблиц-справочников и измерений
sales_genders = BigQueryOperator(
    task_id='sales_genders',
    use_legacy_sql=False,
    sql='call sales.update_lookup("sales.genders","sales.dim_genders", "gender_id", "gender_name", "Gender", "{{ ds }}" )',
    dag=dag
)

# Raw to nf and fact
raw_to_nf = BigQueryOperator(
    task_id='raw_to_nf',
    use_legacy_sql=False,
    sql='call sales.raw_to_nf("{{ ds }}" )',
    dag=dag
)

raw_to_fact = BigQueryOperator(
    task_id='raw_to_fact',
    use_legacy_sql=False,
    sql='call sales.raw_to_fact("{{ ds }}" )',
    dag=dag
)

# удаление csv файла в архив
move_file = LocalFilesystemToGCSOperator(
    task_id='move_file',
    src='/home/airflow/gcs/data/supermarket_sales.csv',
    dst='archive/{{ ds_nodash }}.csv',
    bucket='sample-sales-23',
    # google_cloud_storage_conn_id='google_cloud_default',
    dag=dag
)



download_from_mockaroo >> checkpoint_run >> load_valid_to_bq
load_valid_to_bq >> sales_branches >> sales_cities >> sales_product_lines >> sales_payment_types
sales_payment_types >> sales_member_statuses >> sales_genders
sales_genders >> raw_to_nf >> raw_to_fact >> move_file


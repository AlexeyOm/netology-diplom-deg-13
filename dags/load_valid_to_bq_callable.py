def get_good_rows_load_to_bq():
	import os
	import json
	import pandas as pd
	from google.cloud import bigquery
	from google.cloud import storage

	project="netologyds"
	bucket_name = "sample-sales-23"
	folder_path = "validation/sales_data_expectations"

	# В folder_path ищем последний json файл, загружаем в json_data

	client = storage.Client()
	bucket = client.get_bucket(bucket_name)
	prefix = folder_path if folder_path.endswith('/') else f"{folder_path}/"
	blobs = bucket.list_blobs(prefix=prefix)
	folders = set()
	for blob in blobs:
	    folders.add(blob.name)
	json_file = sorted(list(folders))[-1]


	blob = bucket.get_blob(json_file)
	json_data = json.loads(blob.download_as_text())


	# Складываем в unexpected_index_list номера всех невалидных строк

	unexpected_index_list = set()
	for result in json_data['results']:
	    if 'unexpected_index_list' in result['result']:
	        unexpected_index_list.update(result['result']['unexpected_index_list'])


	print(f'{unexpected_index_list=}')

	invalid_rows = list(unexpected_index_list)

	client = bigquery.Client()

	file_path = '/home/airflow/gcs/data/supermarket_sales.csv'

	df = pd.read_csv(file_path)

	# Remove the invalid rows
	df_cleaned = df.loc[~df.index.isin(invalid_rows)].copy()

	df_cleaned.rename(columns={"gross margin percentage": "gross_margin_percentage",
	                           "Unit price": "Unit_price",
	                           "Invoice ID": "Invoice_ID",
	                           "Product line": "Product_line",
	                           "Customer type": "Customer_type",
	                           "Tax 5%": "Tax_5_",
	                           "gross income": "gross_income"
	                           }, 
	                  inplace=True,
	                 )

	table_ref = client.dataset('sales').table('raw-data')
	table = client.get_table(table_ref)

	errors = client.insert_rows_from_dataframe(table, df_cleaned)

	if errors:
	    print(f'Errors: {errors}')
	    return 1
	else:
	    print('Data loaded successfully!')
	    return 0
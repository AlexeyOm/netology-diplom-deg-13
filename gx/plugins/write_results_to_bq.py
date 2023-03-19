from great_expectations.checkpoint.actions import ValidationAction
from datetime import date
import json
from google.cloud import bigquery


dataset_name = 'sales'
table_name = 'data_loading_log'

class LogToDatabaseResultsAction(ValidationAction):

    def __init__(self, data_context):
        super().__init__(data_context)


    def _run(self, validation_result_suite, **kwargs):
        # Складываем в unexpected_index_list номера всех невалидных строк

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



        unexpected_index_list = set()
        for result in json_data['results']:
            if 'unexpected_index_list' in result['result']:
                unexpected_index_list.update(result['result']['unexpected_index_list'])
        invalid_rows = list(unexpected_index_list)

        
        client = bigquery.Client()
        table_ref = client.dataset(dataset_name).table(table_name)
        table = client.get_table(table_ref)

        rows_fetched = json_data['results'][0]['result']['observed_value'] # первый результат - проверка числа строк в файле
        result_row = [{'loading_date': date.today().strftime("%Y-%m-%d"),
                       'rows_fetched': rows_fetched,
                       'invalid_rows': len(invalid_rows),
                       'invalid_rows_num': ','.join(str(i) for i in invalid_rows)
                     }]

        errors = client.insert_rows(table, result_row)

        if errors:
            print(f'Errors: {errors}')
            return 1
        else:
            print('Data loaded successfully!')
            return 0

            print(f'{unexpected_index_list=}')

        
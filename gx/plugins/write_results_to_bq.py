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

        unexpected_index_list = set()
        for result in validation_result_suite['results']:
            if 'unexpected_index_list' in result['result']:
                unexpected_index_list.update(result['result']['unexpected_index_list'])
        invalid_rows = list(unexpected_index_list)

        
        client = bigquery.Client()
        table_ref = client.dataset(dataset_name).table(table_name)
        table = client.get_table(table_ref)

        rows_fetched = validation_result_suite['results'][0]['result']['observed_value'] # первый результат - проверка числа строк в файле
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

        
import pandas as pd
# разделение датасета на два в зависимости от результатов проверки Great Expectations
import json

with open('path/to/json_file.json', 'r') as f:
    json_data = json.load(f)

# Extract partial_unexpected_index_list values into a set
partial_unexpected_index_list = set()
for result in json_data['results']:
    partial_unexpected_index_list.update(result['result']['partial_unexpected_index_list'])

df = pd.read_csv('path/to/data_file.csv')

good_df = df[~df.index.isin(partial_unexpected_index_list)]
bad_df = df[df.index.isin(partial_unexpected_index_list)]

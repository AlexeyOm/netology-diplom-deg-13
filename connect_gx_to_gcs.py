datasource_yaml = rf"""
name: my_gcs_datasource
class_name: Datasource
execution_engine:
    class_name: PandasExecutionEngine
data_connectors:
    default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
            - default_identifier_name
    default_inferred_data_connector_name:
        class_name: InferredAssetGCSDataConnector
        bucket_or_name: europe-west1-sales-env-e9fc4645-bucket
        prefix: data
        default_regex:
            pattern: (.*)\.csv
            group_names:
                - data_asset_name
"""



import great_expectations as gx
from great_expectations.core.batch import BatchRequest

context = gx.get_context()

batch_request = BatchRequest(
    datasource_name="my_gcs_datasource",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="data/supermarket_sales"
)
context.add_or_update_expectation_suite(expectation_suite_name="test_gcs_suite")
validator = context.get_validator(batch_request=batch_request, expectation_suite_name="test_gcs_suite")

validator.expect_column_values_to_not_be_null(column="Invoice ID")
validator.expect_column_values_to_be_between(
    column="Rating", min_value=1, max_value=10
)

validator.save_expectation_suite(discard_failed_expectations=False)





my_checkpoint_name = "gcs_checkpoint"
checkpoint_config = f"""
name: {my_checkpoint_name}
config_version: 1.0
class_name: SimpleCheckpoint
run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
validations:
  - batch_request:
      datasource_name: my_gcs_datasource
      data_connector_name: default_inferred_data_connector_name
      data_asset_name: "data/supermarket_sales"
    expectation_suite_name: test_gcs_suite
"""

context.add_or_update_checkpoint(**yaml.load(checkpoint_config))

checkpoint_result = context.run_checkpoint(
    checkpoint_name=my_checkpoint_name,
)
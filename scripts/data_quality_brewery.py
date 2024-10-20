import great_expectations as gx
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

output_path = "/opt/airflow/scripts/"
output_file = os.path.join(output_path, "validation_results.txt")

os.makedirs(output_path, exist_ok=True)

context = gx.get_context()

spark = SparkSession.builder \
    .appName("klaus_session") \
    .getOrCreate()

df = spark.read.parquet("/opt/airflow/silver_layer/")

dataframe_datasource = context.sources.add_or_update_spark(
    name="spark_memory"
)

data_asset = dataframe_datasource.add_dataframe_asset(name="dq_test", dataframe=df)

my_batch_request = data_asset.build_batch_request()

expectation_name = "data_quality_expectations"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_name)

validator = context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name=expectation_name
)

successful_validations = 0

with open(output_file, 'w') as f:

    validator.expect_column_values_to_not_be_null(column="id_brewery")

    validator.expect_column_values_to_be_unique(column="id_brewery")

    validator.expect_column_value_lengths_to_equal(column="phone_brewery", value=10)

    validator.expect_column_values_to_be_between(column="latitude_brewery", min_value=-90.0, max_value=90.0)

    validator.expect_column_value_lengths_to_be_between(column="postal_code_brewery", min_value=5, max_value=10)

    results = validator.validate()
    total_validations = len(results['results'])

    for result in results['results']:
        expectation_type = result['expectation_config']['expectation_type']
        column = result['expectation_config']['kwargs'].get('column', 'N/A')
        success = result['success']
        
        if success:
            successful_validations += 1
            
        output = f"Expectativa: {expectation_type} | Coluna: {column} | Sucesso: {success}\n"
        f.write(output)  
        
        if not success:
            failure_details = f"Detalhes da falha: {result}\n"
            f.write(failure_details)
        
        f.write('-' * 60 + '\n')

    success_percentage = (successful_validations / total_validations) * 100

    f.write(f"Percentual de validações bem-sucedidas: {success_percentage:.2f}%\n")

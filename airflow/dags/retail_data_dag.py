"""
Retail Data Pipeline DAG

This DAG orchestrates the retail data pipeline:
1. Triggers data generation via Cloud Function
2. Creates or updates BigQuery tables
3. Loads data from GCS to BigQuery
4. Performs data quality checks
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
import datetime

# Default arguments for DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'retail_data_pipeline',
    default_args=default_args,
    description='Pipeline for retail data processing',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=days_ago(1),
    tags=['retail', 'data-engineering'],
) as dag:

    # 1. Generate synthetic data
    generate_data = CloudFunctionInvokeOperator(
        task_id='generate_synthetic_data',
        function_id='generate-data-function',
        location='europe-west1',
        project_id='sound-machine-457008-i6',
        input_data={},
    )

    # 2. Create or update BigQuery tables if needed
    create_tables = BigQueryExecuteQueryOperator(
        task_id='create_bq_tables',
        sql='''
        -- Create dataset if it doesn't exist
        CREATE SCHEMA IF NOT EXISTS retail_data;

        -- Create customers table
        CREATE TABLE IF NOT EXISTS retail_data.customers (
          customer_id STRING,
          company_name STRING,
          vat_number STRING,
          address STRING,
          postal_code STRING,
          city STRING,
          country STRING,
          email STRING,
          phone STRING,
          industry STRING,
          created_at TIMESTAMP,
          last_modified TIMESTAMP,
          customer_segment STRING,
          is_active BOOLEAN,
          modification_history STRING,
          _file_name STRING,
          _load_time TIMESTAMP
        )
        PARTITION BY DATE(last_modified);

        -- Create products table
        CREATE TABLE IF NOT EXISTS retail_data.products (
          product_id STRING,
          product_name STRING,
          category STRING,
          subcategory STRING,
          price FLOAT64,
          cost FLOAT64,
          weight_kg FLOAT64,
          in_stock BOOLEAN,
          created_at TIMESTAMP,
          _file_name STRING,
          _load_time TIMESTAMP
        )
        PARTITION BY DATE(created_at);

        -- Create orders table
        CREATE TABLE IF NOT EXISTS retail_data.orders (
          order_id STRING,
          customer_id STRING,
          product_id STRING,
          order_date TIMESTAMP,
          quantity INT64,
          status STRING,
          payment_method STRING,
          shipping_method STRING,
          shipping_cost FLOAT64,
          amount FLOAT64,
          total_amount FLOAT64,
          _file_name STRING,
          _load_time TIMESTAMP
        )
        PARTITION BY DATE(order_date);

        -- Create master customers table
        CREATE TABLE IF NOT EXISTS retail_data.customers_master (
          customer_id STRING,
          company_name STRING,
          vat_number STRING,
          address STRING,
          postal_code STRING,
          city STRING,
          country STRING,
          email STRING,
          phone STRING,
          industry STRING,
          created_at TIMESTAMP,
          last_modified TIMESTAMP,
          customer_segment STRING,
          is_active BOOLEAN,
          modification_history STRING,
          _file_name STRING,
          _load_time TIMESTAMP
        );
        ''',
        use_legacy_sql=False,
        location='europe-west1',
    )

    # 3. Load customers data
    load_customers = BigQueryExecuteQueryOperator(
        task_id='load_customers',
        sql='''
        LOAD DATA INTO retail_data.customers
        FROM FILES (
          format = 'CSV',
          uris = ['gs://retail-data-landing-zone/customers/customers_{{ ds }}.csv'],
          skip_leading_rows = 1
        )
        WITH TRANSFORMATION (
          '_file_name', 'customers_{{ ds }}.csv',
          '_load_time', CURRENT_TIMESTAMP()
        );
        ''',
        use_legacy_sql=False,
    )

    # 4. Load products data
    load_products = BigQueryExecuteQueryOperator(
        task_id='load_products',
        sql='''
        LOAD DATA INTO retail_data.products
        FROM FILES (
          format = 'CSV',
          uris = ['gs://retail-data-landing-zone/products/products_{{ ds }}.csv'],
          skip_leading_rows = 1
        )
        WITH TRANSFORMATION (
          '_file_name', 'products_{{ ds }}.csv',
          '_load_time', CURRENT_TIMESTAMP()
        );
        ''',
        use_legacy_sql=False,
    )

    # 5. Load orders data
    load_orders = BigQueryExecuteQueryOperator(
        task_id='load_orders',
        sql='''
        LOAD DATA INTO retail_data.orders
        FROM FILES (
          format = 'CSV',
          uris = ['gs://retail-data-landing-zone/orders/orders_{{ ds }}.csv'],
          skip_leading_rows = 1
        )
        WITH TRANSFORMATION (
          '_file_name', 'orders_{{ ds }}.csv',
          '_load_time', CURRENT_TIMESTAMP()
        );
        ''',
        use_legacy_sql=False,
    )

    # 6. Update master table (only on Mondays)
    update_master = BigQueryExecuteQueryOperator(
        task_id='update_master_table',
        sql='''
        {% if execution_date.weekday() == 0 %}
        -- Only update master on Mondays
        TRUNCATE TABLE retail_data.customers_master;

        LOAD DATA INTO retail_data.customers_master
        FROM FILES (
          format = 'CSV',
          uris = ['gs://retail-data-landing-zone/master/customers_master.csv'],
          skip_leading_rows = 1
        )
        WITH TRANSFORMATION (
          '_file_name', 'customers_master.csv',
          '_load_time', CURRENT_TIMESTAMP()
        );
        {% else %}
        -- Do nothing on other days
        SELECT 1;
        {% endif %}
        ''',
        use_legacy_sql=False,
    )

    # 7. Run data quality checks
    data_quality_checks = BigQueryExecuteQueryOperator(
        task_id='data_quality_checks',
        sql='''
        -- Check for missing values in key fields
        SELECT
          'customers' as table_name,
          COUNT(*) as total_records,
          COUNTIF(customer_id IS NULL) as missing_ids,
          COUNTIF(company_name IS NULL) as missing_names
        FROM retail_data.customers
        WHERE DATE(_load_time) = CURRENT_DATE()

        UNION ALL

        SELECT
          'products' as table_name,
          COUNT(*) as total_records,
          COUNTIF(product_id IS NULL) as missing_ids,
          COUNTIF(product_name IS NULL) as missing_names
        FROM retail_data.products
        WHERE DATE(_load_time) = CURRENT_DATE()

        UNION ALL

        SELECT
          'orders' as table_name,
          COUNT(*) as total_records,
          COUNTIF(order_id IS NULL) as missing_ids,
          COUNTIF(customer_id IS NULL) as missing_customer_ids
        FROM retail_data.orders
        WHERE DATE(_load_time) = CURRENT_DATE();
        ''',
        use_legacy_sql=False,
    )

    # Define task dependencies
    generate_data >> create_tables
    create_tables >> [load_customers, load_products, load_orders]
    [load_customers, load_products, load_orders] >> update_master
    update_master >> data_quality_checks
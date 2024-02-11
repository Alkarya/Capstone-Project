from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'Alkarya',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('airflow_dag',
          default_args=default_args,
          description='Loads and transforms data in Redshift',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

stage_sales_to_redshift = StageToRedshiftOperator( 
    task_id='Stage_sales',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='sales_nano', 
    s3_prefix='sales_data', 
    table='sales_data_sample', 
    copy_options='CSV IGNOREHEADER 1'
)

stage_cities_to_redshift = StageToRedshiftOperator(
    task_id='Stage_cities',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='sales_nano',
    s3_prefix='cities_data', 
    table='worldcities',
    copy_options="FORMAT AS JSON 'auto'"
)

load_sales_fact_table = LoadFactOperator( 
    task_id='load_sales_fact_table',
    dag=dag,
    table='fact_sales',
    redshift_conn_id='redshift',
    sql=SqlQueries.sales_table_insert
)

load_product_dimension_table = LoadDimensionOperator( 
    task_id='load_product_dimension_table',
    dag=dag,
    table='dim_products',
    redshift_conn_id='redshift',
    sql=SqlQueries.products_table_insert
)

load_customer_dimension_table = LoadDimensionOperator( 
    task_id='load_customer_dimension_table',
    dag=dag,
    table='dim_customer',
    redshift_conn_id='redshift',
    sql=SqlQueries.customer_table_insert
)

load_date_dimension_table = LoadDimensionOperator( 
    task_id='load_date_dimension_table',
    dag=dag,
    table='dim_date',
    redshift_conn_id='redshift',
    sql=SqlQueries.date_table_insert
)

load_cities_dimension_table = LoadDimensionOperator( 
    task_id='load_cities_dimension_table',
    dag=dag,
    table='dim_cities',
    redshift_conn_id='redshift',
    sql=SqlQueries.cities_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['fact_sales', 'dim_date', 'dim_customer', 'dim_products', 'dim_cities']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## DAG dependencies

# 1. Staging phase 
start_operator >> [stage_sales_to_redshift,stage_cities_to_redshift]

# 2. Fact phase
[stage_sales_to_redshift,stage_cities_to_redshift] >> load_sales_fact_table

# 3. Dimension phase 
load_sales_fact_table >> [load_product_dimension_table,load_customer_dimension_table,load_cities_dimension_table,load_date_dimension_table]

# 4. Data Quality phase
[load_product_dimension_table,load_customer_dimension_table,load_cities_dimension_table,load_date_dimension_table] >> run_quality_checks

# 5. End Phase
run_quality_checks >> end_operator
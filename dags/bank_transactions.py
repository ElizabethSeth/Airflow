from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pendulum import datetime, duration
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext
from pyspark import SparkContext
from lib.clickhouse_operator_extended import ClickHouseOperatorExtended 

PYSPARK_CONN_ID = "spark"
ram = 25



packages = [
            "com.clickhouse.spark:clickhouse-spark-runtime-3.5_2.12:0.8.0"
            ,"com.clickhouse:clickhouse-jdbc:0.7.0"
            ,"com.clickhouse:clickhouse-client:0.7.0"
            ,"com.clickhouse:clickhouse-http-client:0.7.0"
            ,"org.apache.httpcomponents.client5:httpclient5:5.3.1"
            ,'org.apache.sedona:sedona-spark-3.5_2.12:1.7.0'
            ,'org.datasyslab:geotools-wrapper:1.7.0-28.5'
            ,'uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.4'
        ]





default_args = {
    "owner": "Smalch",
    "depends_on_past": False,
    "retries": 0
}

@dag(
    tags=["test", "transactions"],
    render_template_as_native_obj=True,
    max_active_runs=1,
    schedule=None,
    default_args=default_args,
    start_date=datetime(2023, 12, 1),
    catchup=False,
    description='testing connection',
    template_searchpath='dags/include',
    doc_md=__doc__
)
def pyspark_test_work():
    spark_submit_task = SparkSubmitOperator(
        task_id='bank_transactions',
        application='dags/spark_app/app_test.py',
        conn_id=PYSPARK_CONN_ID,
        packages=','.join(packages),
        #executor_cores='1',
        executor_memory=f'{ram}g',
        driver_memory=f'{ram}g',
        verbose=True
    )
    spark_submit_task
   
pyspark_test_work()
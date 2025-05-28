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

import os

CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_CONN_ID = 'clickhouse'
PYSPARK_CONN_ID = "spark"


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

ram = 2




@dag(
    tags=["test", "stocks"],
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
def spark_clickhouse_test():

    current_time = ClickHouseOperatorExtended(
        task_id='current_time',
        clickhouse_conn_id=CLICKHOUSE_CONN_ID,
        sql='test.sql'
    )

    @task.pyspark(
        conn_id=PYSPARK_CONN_ID,
        config_kwargs={
                'spark.jars.packages':','.join(packages)
                ,'spark.executor.memory': f'{ram}g'
                ,'spark.driver.memory': f'{ram}g'
                ,"spark.driver.maxResultSize": f"{ram}g"
                ,"spark.executor.memoryOverhead": f"{ram}g"
        }
    )
    def run_spark_task(spark: SparkSession, sc: SparkContext):
        config = (
            SedonaContext.builder()
            .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
            .config("spark.sql.catalog.clickhouse.host", CH_IP)
            .config("spark.sql.catalog.clickhouse.protocol", "http")
            .config("spark.sql.catalog.clickhouse.http_port", "8123")
            .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
            .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
            .config("spark.sql.catalog.clickhouse.database", "default")
            .config("spark.clickhouse.write.format", "json")
            .getOrCreate()
        )


        spark = SedonaContext.create(config)
        sc = spark.sparkContext
        spark.sql("USE clickhouse")
        df = spark.sql("SELECT * FROM card_data.darknet_data LIMIT 5")
        df.show()

    spark_submit_task = SparkSubmitOperator(
        task_id='spark_submit_job',
        application='dags/spark_app/spark_1.py',
        conn_id=PYSPARK_CONN_ID,
        packages=','.join(packages),
        #executor_cores='1',
        executor_memory=f'{ram}g',
        driver_memory=f'{ram}g',
        verbose=True
    )

    #[connect_to_clickhouse(), current_time] >> [spark_submit_task, run_spark_task()]
    spark_submit_task >> run_spark_task() >> current_time

spark_clickhouse_test()








# default_args={
#     "owner": "Smalch",
#     "depends_on_past": False,
#     "retries": 0
# }
# ram = 25
# #cpu = 30*3
# @dag(
#     tags=["test", "stocks"],
#     render_template_as_native_obj=True,
#     max_active_runs=1,
#     #schedule='50 2 * * *',
#     schedule=None,
#     default_args=default_args,
#     start_date=datetime(2023, 12, 1),
#     catchup=False,
#     description='testing connection',
#     doc_md=__doc__
# )
# def spark_clickhouse_test():
#     spark_submit_task = SparkSubmitOperator(
#         task_id='spark_submit_job',
#         application='dags/spark_app/spark_1.py',
#         #conn_id='spark_master',
#         conn_id='spark',
#         #total_executor_cores='1',
#         #executor_cores='1',
#         executor_memory=f'{ram}g',
#         #num_executors='1',
#         driver_memory=f'{ram}g',
#         verbose=True
#     )
#     spark_submit_task

# spark_clickhouse_test()
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CLICKHOUSE_DB = "card_data"
CLICKHOUSE_TABLE = "transactions"
CH_PORT = os.getenv('CH_PORT')

ram = 25

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName('ClickHouseSparkIntegration')
        .config("spark.jars.repositories", "https://artifacts.unidata.ucar.edu/repository/unidata-all")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", CH_IP)
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port", CH_PORT)
        .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
        .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
        .config("spark.sql.catalog.clickhouse.database", CLICKHOUSE_DB)
        .config("spark.driver.maxResultSize", f"{ram}g")
        .config("spark.executor.memoryOverhead", f"{ram}g")
        .getOrCreate()
    )
    
    SedonaRegistrator.registerAll(spark)
    return spark


def schema_table(df):
    schema = []
    for field in df.schema.fields:
        type_table = "String" if field.dataType.simpleString() == "string" else "Float64"
        schema.append(f"{field.name} {type_table}")
    return ", ".join(schema)
    
def preprocess_data(spark, path_data):
    df = spark.read.text(path_data)
    df = df.withColumn("unique_id", monotonically_increasing_id())

    split_df = df.withColumn("split_values", F.split(F.col("value"), ","))

    column_count = len(split_df.select("split_values").first()[0])
    column_names = [f"col_{i}" for i in range(column_count)]

    process_df = split_df.select(
        [F.col("split_values").getItem(i).alias(column_names[i]) for i in range(column_count)]
    )

    return process_df, column_names
    
def drop_and_create_table(spark, schema):
    spark.sql(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
    spark.sql(f"""
                CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
                ({schema}) 
                ENGINE = MergeTree()
                ORDER BY col_0
                """)
    

def write_to_clickhouse(df):
    df.writeTo(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").append()

def main():
    spark = create_spark_session()
    spark.sql(f"USE {CLICKHOUSE_DB}")
    base = '/opt/airflow/airflow'
    path_data = f"{base}/data"
    process_df, column_names = preprocess_data(spark, f"{path_data}/HI-Medium_Patterns.txt")
    schema = schema_table(process_df)
    drop_and_create_table(spark, schema)
    write_to_clickhouse(process_df)

    spark.sql(f"SELECT * FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").show(truncate=False)

if __name__ == "__main__":
    main()


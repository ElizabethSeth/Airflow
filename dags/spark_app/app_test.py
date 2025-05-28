from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from pyspark.sql.window import Window
import pandas as pd
import geopandas as gpd
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import monotonically_increasing_id



CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')


ram = 25

def main():
    spark = (
        SparkSession.builder
            .appName('appName')
            
            .config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all')
            .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
            .config("spark.sql.catalog.clickhouse.host", f"{CH_IP}")
            .config("spark.sql.catalog.clickhouse.protocol", "http")
            .config("spark.sql.catalog.clickhouse.http_port", "8123")
            .config("spark.sql.catalog.clickhouse.user", f"{CLICKHOUSE_USER}")
            .config("spark.sql.catalog.clickhouse.password", f"{CLICKHOUSE_PASSWORD}")
            .config("spark.sql.catalog.clickhouse.database", "default")
            .config("spark.driver.maxResultSize", f"{ram}g")
            .config("spark.executor.memoryOverhead", f"{ram}g")
            .getOrCreate()
        )

    SedonaRegistrator.registerAll(spark)
    spark.sql("use clickhouse")


    base = '/opt/airflow/airflow'
    path_data = f"{base}/data"

    df = spark.read.text(f"{path_data}/HI-Medium_Patterns.txt")
    df = df.withColumn("unique_id", monotonically_increasing_id())

    df = df.withColumn("Suspicious_Type", 
                    F.when(F.col("value").rlike(r"BEGIN LAUNDERING ATTEMPT.*"), 
                            F.split(F.col("value"), " - ").getItem(1))
                    .otherwise(None))

    window_spec = Window.orderBy("unique_id").rowsBetween(Window.unboundedPreceding, 0)

    df_filled = df.withColumn("Suspicious_Type", 
                            F.last("Suspicious_Type", True).over(window_spec))

    df_cleaned = df_filled.filter(~F.col("value").rlike(r"BEGIN LAUNDERING ATTEMPT|END LAUNDERING ATTEMPT"))

    split_df = df_cleaned.withColumn("split_values", F.split(F.col("value"), ","))

    processed_df = split_df.select(
        F.col("split_values").getItem(0).alias("Timestamp"),
        F.col("split_values").getItem(1).alias("From_ID"),
        F.col("split_values").getItem(2).alias("From_Account"),
        F.col("split_values").getItem(3).alias("To_ID"),
        F.col("split_values").getItem(4).alias("To_Account"),
        F.col("split_values").getItem(5).alias("Amount"),
        F.col("split_values").getItem(6).alias("Currency"),
        F.col("split_values").getItem(7).alias("Converted_Amount"),
        F.col("split_values").getItem(8).alias("Converted_Currency"),
        F.col("split_values").getItem(9).alias("Transaction_Type"),
        F.col("split_values").getItem(10).alias("Flag"),
        F.col("Suspicious_Type")
    )

    processed_df = processed_df.filter(F.col("Timestamp").rlike(r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}"))

    processed_df.writeTo("card_data.bank_transactions").append()
    
    spark.sql('select * from card_data.bank_transactions').show(truncate=False)

    
if __name__ == "__main__":
    main()



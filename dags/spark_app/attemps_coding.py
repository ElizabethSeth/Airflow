import builtins
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from sedona.register import SedonaRegistrator
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
import clickhouse_connect

CH_IP = os.getenv('CH_IP')
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
CH_PORT = os.getenv('CH_PORT')

CLICKHOUSE_DB = "card_data"
CLICKHOUSE_TABLE = "transactions"

#base = '/home/wcuhhhunter/airflow'
base = '/opt/airflow/airflow'
path_data = f"{base}/data"
txt_path = f"{path_data}/HI-Medium_Patterns.txt"

ram = 25

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName('clickhouse_test')
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
    
    #print(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD)
    client = clickhouse_connect.get_client(host=CH_IP, port=CH_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    #client=None
    SedonaRegistrator.registerAll(spark)
    spark.sql("use clickhouse")
    return spark, client




def full_func_creation_table(df, db, table):
#df.printSchema()
    null_counts = {}
    for col_name in df.columns:
        null_count = (df.withColumn("new_column", F.when((F.col(col_name).isNull()) | (F.isnan(F.col(col_name))), 1).otherwise(0)).agg(F.sum("new_column").alias("temp_sum")).collect()[0][0])

        null_counts[col_name] = null_count
    total_count = builtins.sum(null_counts.values())


    types = {"string": "String"}
    columns = [f"{column_name} {types.get(column_type, 'String')}" for column_name, column_type in df.dtypes]
    return f"create table if not exists {db}.{table} ({','.join(columns)}) ENGINE=MergeTree() order by {df.columns[0]}"


def loading_to_db(client, df, dataframe, db, table):
    client.command(f'drop table if exists {db}.{table}')
    client.command(df)
    dataframe.writeTo(f"{db}.{table}").append()


def creation_table(spark):
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

    #processed_df.show(truncate=False)
    return processed_df

def main():
    spark, client = create_spark_session()
    processed_df = creation_table(spark)
    df  = full_func_creation_table(processed_df,CLICKHOUSE_DB, CLICKHOUSE_TABLE)
    loading_to_db(client, df, processed_df, CLICKHOUSE_DB, CLICKHOUSE_TABLE )
    
if __name__ == "__main__":
    main()



# def preprocess_data(spark, path_data):
#     df = spark.read.csv(path_data, header=True, inferSchema=True)
#     df = df.withColumn("unique_id", monotonically_increasing_id())
#     return df, df.columns

# def schema_table(df):
#     schema = []
#     for field in df.schema.fields:
#         spark_type = field.dataType.simpleString()
#         ch_type = "String"
#         if "int" in spark_type:
#             ch_type = "Int"
#         elif "double" in spark_type or "float" in spark_type:
#             ch_type = "Float"
#         schema.append(f"{field.name} {ch_type}")
#     return ", ".join(schema)

# def drop_and_create_table(client, schema, column_names):
#     client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
#     client.command(f"""
#                 CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
#                 ({schema}) 
#                 ENGINE = MergeTree() 
#                 ORDER BY {column_names[0]}
#                 """)

# def write_to_clickhouse(df):
#     df.writeTo(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").append()

# def main():
#     spark, client = create_spark_session()
#     base = '/opt/airflow/airflow'
#     path_data = f"{base}/data/LI-Small_Trans.csv"

#     process_df, column_names = preprocess_data(spark, path_data)
    
#     schema = schema_table(process_df)
#     drop_and_create_table(client, schema, column_names)
#     write_to_clickhouse(process_df)

#     spark.sql(f"SELECT * FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").show(truncate=False)

# if __name__ == "__main__":
#     main()



















# from pyspark.sql import SparkSession
# import os
# from pyspark.sql import functions as F
# from sedona.register import SedonaRegistrator
# from pyspark.sql.window import Window
# from pyspark.sql.functions import monotonically_increasing_id
# import clickhouse_connect
# CH_IP = os.getenv('CH_IP')
# CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
# CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
# CH_PORT = os.getenv('CH_PORT')

# CLICKHOUSE_DB = "card_data"
# CLICKHOUSE_TABLE = "transactions"

# ram = 25

# def create_spark_session():
#     spark = (
#         SparkSession.builder
#         .appName('clickhouse_test')
#         .config("spark.jars.repositories", "https://artifacts.unidata.ucar.edu/repository/unidata-all")
#         .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
#         .config("spark.sql.catalog.clickhouse.host", CH_IP)
#         .config("spark.sql.catalog.clickhouse.protocol", "http")
#         .config("spark.sql.catalog.clickhouse.http_port", CH_PORT)
#         .config("spark.sql.catalog.clickhouse.user", CLICKHOUSE_USER)
#         .config("spark.sql.catalog.clickhouse.password", CLICKHOUSE_PASSWORD)
#         .config("spark.sql.catalog.clickhouse.database", CLICKHOUSE_DB)
#         .config("spark.driver.maxResultSize", f"{ram}g")
#         .config("spark.executor.memoryOverhead", f"{ram}g")
#         .getOrCreate()
#     )
    


#     client = clickhouse_connect.get_client(host=CH_IP, port=CH_PORT, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    

#     SedonaRegistrator.registerAll(spark)
#     spark.sql("use clickhouse")
#     return spark, client

# def schema_table(df):
#     schema = []
#     for field in df.schema.fields:
#         type_table = "String" if field.dataType.simpleString() == "string" else "Float64"
#         schema.append(f"{field.name} {type_table}")
#     return ", ".join(schema)
    
# def preprocess_data(spark, path_data):
#     df = spark.read.text(path_data)
#     df = df.withColumn("unique_id", monotonically_increasing_id())

#     split_df = df.withColumn("split_values", F.split(F.col("value"), ","))

#     column_count = len(split_df.select("split_values").first()[0])
#     column_names = [f"col_{i}" for i in range(column_count)]

#     process_df = split_df.select(
#         [F.col("split_values").getItem(i).alias(column_names[i]) for i in range(column_count)]
#     )

#     return process_df, column_names
    
# def drop_and_create_table(client, schema):
#     client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
#     client.command(f"""
#                 CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
#                 ({schema}) 
#                 ENGINE = MergeTree() 
#                 ORDER BY col_0
#                 """)
    

# def write_to_clickhouse(df):
#     df.writeTo(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").append()

# def main():
#     spark, client = create_spark_session()
#     base = '/opt/airflow/airflow'
#     path_data = f"{base}/data"
#     process_df, column_names = preprocess_data(spark, f"{path_data}/HI-Medium_Patterns.txt")
#     schema = schema_table(process_df)
#     drop_and_create_table(client, schema)
#     write_to_clickhouse(process_df)

#     spark.sql(f"SELECT * FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").show(truncate=False)

# if __name__ == "__main__":
#     main()


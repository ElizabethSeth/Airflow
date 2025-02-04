from pyspark.sql import SparkSession
import os
def run():
    spark = SparkSession.builder.appName("ExampleJob111").getOrCreate()
    root = "/opt/airflow"
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    df.write.parquet(f"{root}/test/test", mode="overwrite")
    print(os.listdir(root))
    #df = spark.read.csv(f"{root}/test/df_test.csv", header=True, inferSchema=True)
    df.show()


    #show all folders in the current directory
    

    
    df.toPandas().to_csv(f"{root}/test/df_test.csv", index=False)
    spark.stop()
    return



if __name__ == "__main__":
    run()
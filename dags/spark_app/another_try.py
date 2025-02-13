

def parse_laundering_file(spark, file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    data = []
    current_attempt_type = None

    for line in lines:
        line = line.strip()

        if line.startswith("BEGIN LAUNDERING ATTEMPT -"):
            current_attempt_type = line.replace("BEGIN LAUNDERING ATTEMPT -", "").strip()
            continue
        if line.startswith("END LAUNDERING ATTEMPT"):
            current_attempt_type = None
            continue
        if current_attempt_type and line:
            data.append(line.split(",") + [current_attempt_type])
    columns = ["Date", "Time", "Transaction_ID", "User_ID", "Account", "Amount", "Currency", "Payment_Type", "Status", "Attempt_Type"]
    df = spark.createDataFrame(data, columns)
    return df

def schema_table(df):
    schema = []
    for field in df.schema.fields:
        spark_type = field.dataType.simpleString()
        ch_type = "String"
        if "int" in spark_type:
            ch_type = "Int64"
        elif "double" in spark_type or "float" in spark_type:
            ch_type = "Float64"
        schema.append(f"{field.name} {ch_type}")
    return ", ".join(schema)

def drop_and_create_table(client, schema, column_names):
    client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
    client.command(f"""
                CREATE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
                ({schema}) 
                ENGINE = MergeTree() 
                ORDER BY {column_names[0]}
                """)

def write_to_clickhouse(df):
    df.writeTo(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").append()

def main(file_path):

    spark, client = create_spark_session()
    process_df = parse_laundering_file(spark, file_path)
    schema = schema_table(process_df)
    drop_and_create_table(client, schema, process_df.columns)
    write_to_clickhouse(process_df)
    spark.sql(f"SELECT * FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}").show(truncate=False)

if __name__ == "__main__":


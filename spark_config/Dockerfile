FROM bitnami/spark:3.5.4-debian-12-r0

USER root

# Install wget (or curl) so we can pull down JARs
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*

# Define environment variables for clarity
ENV CLICKHOUSE_SPARK_VERSION=0.8.0
ENV CLICKHOUSE_JDBC_VERSION=0.7.1-patch1

# 1) Download the ClickHouse Spark "runtime" JAR that includes the V2 connector
RUN wget -q -O /opt/bitnami/spark/jars/clickhouse-spark-runtime-3.5_2.12-${CLICKHOUSE_SPARK_VERSION}.jar \
    "https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-3.5_2.12/${CLICKHOUSE_SPARK_VERSION}/clickhouse-spark-runtime-3.5_2.12-${CLICKHOUSE_SPARK_VERSION}.jar"

# 2) Download the ClickHouse JDBC driver "all" jar
RUN wget -q -O /opt/bitnami/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar \
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar"


RUN pip install --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt
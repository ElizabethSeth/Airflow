FROM apache/airflow:2.10.3

USER root

# Install OpenJDK-17
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

    

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install apache-airflow==${AIRFLOW_VERSION} --no-cache-dir -r requirements.txt


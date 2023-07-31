FROM apache/airflow:2.6.3

USER root

# Install gosu for switching to root temporarily
RUN apt-get update && apt-get install -y gosu

# Install Java
RUN apt-get update && apt-get install -y default-jdk

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/default-java

USER airflow

# Define the PostgreSQL JDBC driver version
ENV POSTGRES_JDBC_VERSION 42.6.0

# Create dir for jdbc maven driver
RUN mkdir -p /opt/airflow/drivers/

RUN curl -o /opt/airflow/drivers/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
FROM apache/airflow:2.6.0-python3.10

USER root

# Install Apache Spark
RUN apt-get update
RUN apt-get install --fix-missing -y curl
RUN apt-get install --fix-missing -y wget
RUN apt-get install --fix-missing -y software-properties-common
RUN apt-get install --fix-missing -y ssh
RUN apt-get install --fix-missing -y net-tools
RUN apt-get install --fix-missing -y ca-certificates
RUN apt-get install --fix-missing -y openjdk-11-jre-headless

ENV SPARK_VERSION=3.4.1 \
HADOOP_VERSION=3 \
SPARK_HOME=/opt/spark \
PYTHONHASHSEED=1

RUN wget -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
RUN mkdir -p /opt/spark
RUN tar -xf apache-spark.tgz -C /opt/spark --strip-components=1
RUN rm apache-spark.tgz

USER airflow

# Set environment variables for Apache Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Expose the necessary ports
EXPOSE 4040 8080 18080

# Copy and install Python dependencies
ADD requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt


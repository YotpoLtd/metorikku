ARG SPARK_VERSION=3.2.1
FROM metorikku/spark:base-${SPARK_VERSION}

ARG AWS_SDK_VERSION=1.12.230
ARG HADOOP_VERSION=3.3.3
ARG HTTPCLIENT_VERSION=4.5.11
ARG SCALA_MAJOR_VERSION=2.12
ARG SPARK_VERSION=3.2.1
ARG METRICS_FILE=metrics_spark3.properties

USER root

ADD https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_SDK_VERSION}/aws-java-sdk-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_SDK_VERSION}/aws-java-sdk-core-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/${AWS_SDK_VERSION}/aws-java-sdk-s3-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/${AWS_SDK_VERSION}/aws-java-sdk-dynamodb-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-streaming-kafka-0-10-assembly_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar $SPARK_HOME/jars/
RUN rm -f $SPARK_HOME/jars/httpclient-*.jar
RUN rm -f $SPARK_HOME/jars/guava-*.jar
ADD https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/${HTTPCLIENT_VERSION}/httpclient-${HTTPCLIENT_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar $SPARK_HOME/jars/

# JDBC dependencies.
ADD https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar $SPARK_HOME/jars

# Mongo dependencies.
ADD https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/3.0.1/mongo-spark-connector_2.12-3.0.1.jar $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.12.10/mongo-java-driver-3.12.10.jar $SPARK_HOME/jars

RUN chmod 644 $SPARK_HOME/jars/*

ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar /prometheus/
RUN chmod 644 /prometheus/*.jar

#Python
RUN apt-get update \
    && apt-get install -y wget curl coreutils jq less inotify-tools python3 python3-setuptools \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py 'pip==20.1.1' \
    && rm get-pip.py \
    && rm -rf /var/lib/apt/lists/*

#USER ${spark_uid}

ADD conf/* ${SPARK_HOME}/custom/conf/

# metrics

RUN mkdir -p /etc/metrics/conf
ADD metrics/prometheus.yaml /etc/metrics/conf
ADD metrics/${METRICS_FILE} /etc/metrics/conf/metrics.properties

RUN touch hadoop-metrics2.properties

ENV TINI_KILL_PROCESS_GROUP 1
ENV TINI_SUBREAPER 1
ENV PYTHONHASHSEED 1

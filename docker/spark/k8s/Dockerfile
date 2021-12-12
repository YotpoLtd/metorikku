ARG SPARK_VERSION=3.0.1
FROM metorikku/spark:base-${SPARK_VERSION}

ARG AWS_SDK_VERSION=1.11.853
ARG HADOOP_VERSION=3.2.0
ARG HTTPCLIENT_VERSION=4.5.11
ARG SCALA_MAJOR_VERSION=2.12
ARG SPARK_VERSION=3.0.0

USER root

ADD https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_SDK_VERSION}/aws-java-sdk-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_SDK_VERSION}/aws-java-sdk-core-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/${AWS_SDK_VERSION}/aws-java-sdk-s3-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/${AWS_SDK_VERSION}/aws-java-sdk-dynamodb-${AWS_SDK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-streaming-kafka-0-10-assembly_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_${SCALA_MAJOR_VERSION}/${SPARK_VERSION}/spark-avro_${SCALA_MAJOR_VERSION}-${SPARK_VERSION}.jar $SPARK_HOME/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.6.2/commons-pool2-2.6.2.jar $SPARK_HOME/jars/
RUN rm -f $SPARK_HOME/jars/httpclient-*.jar
ADD https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/${HTTPCLIENT_VERSION}/httpclient-${HTTPCLIENT_VERSION}.jar $SPARK_HOME/jars/

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
ENV SPARK_MAJOR "${SPARK_VERSION:0:1}"
RUN mkdir -p /etc/metrics/conf
ADD metrics/prometheus.yaml /etc/metrics/conf
ADD metrics/metrics_spark${SPARK_MAJOR}.properties /etc/metrics/conf/metrics.properties
ADD metrics/* /etc/metrics/conf/

RUN touch hadoop-metrics2.properties

ENV PYTHONHASHSEED 1

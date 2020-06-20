ARG SPARK_VERSION=3.0.0
ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v${SPARK_VERSION}-gcs-prometheus
FROM ${SPARK_IMAGE}

# Install Tools
RUN apt-get update \
    && apt-get install -y curl wget \
    && rm -rf /var/lib/apt/lists/*

#AWS
ARG AWS_SDK_VERSION=1.11.853
ARG HADOOP_VERSION=3.2.1
ARG HTTPCLIENT_VERSION=4.5.11

RUN wget -q https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_SDK_VERSION}/aws-java-sdk-${AWS_SDK_VERSION}.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_SDK_VERSION}/aws-java-sdk-core-${AWS_SDK_VERSION}.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/${AWS_SDK_VERSION}/aws-java-sdk-s3-${AWS_SDK_VERSION}.jar -P $SPARK_HOME/jars/
RUN rm -f $SPARK_HOME/jars/httpclient-*.jar && wget -q https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/${HTTPCLIENT_VERSION}/httpclient-${HTTPCLIENT_VERSION}.jar -P /spark/jars

#Python
RUN apt-get update \
    && apt-get install -y coreutils jq less inotify-tools python3 python3-setuptools \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py 'pip==20.1.1' \
    && rm get-pip.py \
    && rm -rf /var/lib/apt/lists/*

ADD docker/spark/base/conf/log4j.json.properties ${SPARK_HOME}/custom/conf/
ADD docker/spark/base/conf/atlas-application.properties ${SPARK_HOME}/custom/conf/

ENV PYTHONHASHSEED 1

ENTRYPOINT ["/opt/entrypoint.sh"]
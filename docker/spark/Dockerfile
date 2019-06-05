FROM openjdk:8u212-b04-jre-stretch

# BASE
ARG SPARK_VERSION=2.4.3
ARG HADOOP_VERSION=2.7

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && cd /

RUN apt-get update \
    && apt-get install -y coreutils jq less inotify-tools python3 python3-setuptools \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py 'pip==9.0.1' \
    && rm get-pip.py

RUN wget -q http://central.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar -P /spark/jars/
RUN wget -q http://central.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar -P /spark/jars/
RUN wget -q http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar -P /spark/jars/
RUN wget -q http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar  -P /spark/jars/

ADD log4j.json.properties /spark/conf/
ADD spark-defaults.conf /spark/conf/

ENV PYTHONHASHSEED 1
ENV SPARK_HOME /spark
ENV PATH="/spark/bin:${PATH}"

ADD scripts /scripts
ENTRYPOINT ["/scripts/entrypoint-master.sh"]
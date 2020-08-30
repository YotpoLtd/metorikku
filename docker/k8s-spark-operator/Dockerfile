ARG SCALA_VERSION=2.11
ARG SPARK_VERSION=2.4.5
ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v${SPARK_VERSION}-gcs-prometheus
FROM ${SPARK_IMAGE}
ARG SPARK_VERSION

# Install Tools
RUN apt-get update \
    && apt-get install -y curl wget

#Remove hadoop 2.7 Spark jars
RUN rm -f $SPARK_HOME/jars/hadoop*2.7* && cd /

#Hadoop
ARG HADOOP_VERSION=2.9.2
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

RUN ln -s /opt/hadoop-${HADOOP_VERSION}/etc/hadoop /etc/hadoop
RUN cp /etc/hadoop/mapred-site.xml.template /etc/hadoop/mapred-site.xml
RUN mkdir /opt/hadoop-${HADOOP_VERSION}/logs

ENV HADOOP_PREFIX=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=/etc/hadoop

#Hive
ENV HIVE_HOME=/opt/hive
ENV HIVE_VERSION=2.3.3
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

#Hudi
ENV HUDI_VERSION=0.5.3
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hive-bundle/$HUDI_VERSION/hudi-hive-bundle-$HUDI_VERSION.jar \
    && mv hudi-hive-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/$HUDI_VERSION/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar \
    && mv hudi-hadoop-mr-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib

#AWS
ARG AWS_SDK_VERSION=1.11.699
ARG HADOOP_AWS_VERSION=3.0.3
ARG HTTPCLIENT_VERSION=4.5.11

RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/${SPARK_VERSION}/spark-sql-kafka-0-10_2.11-${SPARK_VERSION}.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.0.0/kafka-clients-2.0.0.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar -P $SPARK_HOME/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P $SPARK_HOME/jars/
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

ADD docker/k8s-spark-operator/conf/log4j.json.properties ${SPARK_HOME}/custom/conf/
ADD docker/k8s-spark-operator/conf/atlas-application.properties ${SPARK_HOME}/custom/conf/
ADD examples ${SPARK_HOME}/work-dir/examples/

ENV PYTHONHASHSEED 1
ENV PATH="${HADOOP_PREFIX}/bin:${SPARK_HOME}/bin:${PATH}"
ENV SPARK_DIST_CLASSPATH /etc/hadoop:/opt/hadoop-2.9.2/share/hadoop/common/lib/*:/opt/hadoop-2.9.2/share/hadoop/common/*:/opt/hadoop-2.9.2/share/hadoop/hdfs:/opt/hadoop-2.9.2/share/hadoop/hdfs/lib/*:/opt/hadoop-2.9.2/share/hadoop/hdfs/*:/opt/hadoop-2.9.2/share/hadoop/yarn:/opt/hadoop-2.9.2/share/hadoop/yarn/lib/*:/opt/hadoop-2.9.2/share/hadoop/yarn/*:/opt/hadoop-2.9.2/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.9.2/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:${SPARK_HOME}/custom/conf/
ENV SPARK_EXTRA_CLASSPATH $SPARK_DIST_CLASSPATH
ENTRYPOINT ["/opt/entrypoint.sh"]
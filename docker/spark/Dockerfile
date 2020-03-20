FROM openjdk:8u212-b04-jre-stretch

ARG SPARK_VERSION=2.4.5

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop2.7 spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop2.7.tgz \
    && rm -f /spark/jars/hadoop*2.7* \
    && cd /

ARG HADOOP_VERSION=2.9.2
    
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

RUN ln -s /opt/hadoop-${HADOOP_VERSION}/etc/hadoop /etc/hadoop
RUN cp /etc/hadoop/mapred-site.xml.template /etc/hadoop/mapred-site.xml
RUN mkdir /opt/hadoop-${HADOOP_VERSION}/logs

ENV HADOOP_PREFIX=/opt/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF_DIR=/etc/hadoop

ENV HIVE_HOME=/opt/hive
ENV HIVE_VERSION=2.3.3
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

ENV HUDI_VERSION=0.5.1-incubating
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hive-bundle/$HUDI_VERSION/hudi-hive-bundle-$HUDI_VERSION.jar \
    && mv hudi-hive-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/$HUDI_VERSION/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar \
    && mv hudi-hadoop-mr-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib

RUN apt-get update \
    && apt-get install -y coreutils jq less inotify-tools python3 python3-setuptools \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py 'pip==9.0.1' \
    && rm get-pip.py \
    && rm -rf /var/lib/apt/lists/*

ARG AWS_SDK_VERSION=1.11.699
ARG HADOOP_AWS_VERSION=3.0.3
ARG HTTPCLIENT_VERSION=4.5.11
    
RUN wget -q https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar -P /spark/jars/
RUN wget -q https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar -P /spark/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P /spark/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/${AWS_SDK_VERSION}/aws-java-sdk-${AWS_SDK_VERSION}.jar -P /spark/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/${AWS_SDK_VERSION}/aws-java-sdk-core-${AWS_SDK_VERSION}.jar -P /spark/jars/
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/${AWS_SDK_VERSION}/aws-java-sdk-s3-${AWS_SDK_VERSION}.jar -P /spark/jars/
RUN rm -f /spark/jars/httpclient-*.jar && wget -q https://repo1.maven.org/maven2/org/apache/httpcomponents/httpclient/${HTTPCLIENT_VERSION}/httpclient-${HTTPCLIENT_VERSION}.jar -P /spark/jars

ADD log4j.json.properties /spark/conf/
ADD spark-defaults.conf /spark/conf/
ADD spark-env.sh /spark/conf/

ENV PYTHONHASHSEED 1
ENV SPARK_HOME /spark
ENV PATH="${HADOOP_PREFIX}/bin:/spark/bin:${PATH}"

ADD scripts /scripts
ENTRYPOINT ["/scripts/entrypoint-master.sh"]
FROM openjdk:8u212-b04-jre-stretch

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=2.7.4
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
    && tar -xzf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV HIVE_HOME=/opt/hive
ENV HIVE_VERSION=1.2.2
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

ENV MYSQL_CONNECTOR_VERSION=5.1.47
RUN wget -q http://central.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_CONNECTOR_VERSION/mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar \
    && mv mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar $HIVE_HOME/lib

ENV HUDI_VERSION=0.4.5
RUN apt-get update && apt-get install -y ant
RUN wget -q http://central.maven.org/maven2/com/uber/hoodie/hoodie-hive-bundle/$HUDI_VERSION/hoodie-hive-bundle-$HUDI_VERSION.jar \
    && mv hoodie-hive-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q http://central.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar \
    && mv hadoop-aws-$HADOOP_VERSION.jar $HIVE_HOME/lib

ENV AWS_JAVA_SDK_VERSION=1.7.4
RUN wget -q http://central.maven.org/maven2/com/amazonaws/aws-java-sdk/$AWS_JAVA_SDK_VERSION/aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar \
    && mv aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar $HIVE_HOME/lib

RUN wget -q http://central.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar \
    && mv jsonevent-layout-1.7.jar $HIVE_HOME/lib
RUN wget -q http://central.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar \
    && mv json-smart-1.1.1.jar $HIVE_HOME/lib

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.5.0/wait /wait
RUN chmod +x /wait

COPY start-hive.sh /
COPY log4j.json.properties .

CMD /wait && /start-hive.sh
FROM openjdk:8-jre

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=2.7.4
RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
    && tar -xzvf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV HIVE_HOME=/opt/hive
ENV HIVE_VERSION=1.2.2
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzvf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

ENV MYSQL_CONNECTOR_VERSION=5.1.47
RUN wget -q http://central.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_CONNECTOR_VERSION/mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar \
    && mv mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar $HIVE_HOME/lib

RUN apt-get update && apt-get install -y ant
RUN wget -q http://central.maven.org/maven2/com/uber/hoodie/hoodie-hive-bundle/0.4.5/hoodie-hive-bundle-0.4.5.jar \
    && mv hoodie-hive-bundle-0.4.5.jar $HIVE_HOME/lib

COPY start-hive.sh /

ENTRYPOINT ["/start-hive.sh"]
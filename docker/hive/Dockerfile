FROM openjdk:8u212-b04-jre-stretch

RUN mkdir /opt/atlas
ENV ATLAS_HOME=/opt/atlas
RUN mkdir -p $ATLAS_HOME/hook/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=2.7.4
RUN apt-get update && apt-get install -y ant && apt-get install -y expect

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
    && tar -xzf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV HIVE_HOME=/opt/hive
ARG HIVE_VERSION=2.3.7
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

ENV MYSQL_CONNECTOR_VERSION=5.1.47
RUN wget -q https://repo1.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_CONNECTOR_VERSION/mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar \
    && mv mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar $HIVE_HOME/lib

ARG HUDI_VERSION=0.5.3
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hive-bundle/$HUDI_VERSION/hudi-hive-bundle-$HUDI_VERSION.jar \
    && mv hudi-hive-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/$HUDI_VERSION/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar \
    && mv hudi-hadoop-mr-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib

RUN wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar \
    && mv hadoop-aws-$HADOOP_VERSION.jar $HIVE_HOME/lib

ENV AWS_JAVA_SDK_VERSION=1.7.4
RUN wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/$AWS_JAVA_SDK_VERSION/aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar \
    && mv aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar $HIVE_HOME/lib

RUN wget -q https://repo1.maven.org/maven2/net/logstash/log4j/jsonevent-layout/1.7/jsonevent-layout-1.7.jar \
    && mv jsonevent-layout-1.7.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/net/minidev/json-smart/1.1.1/json-smart-1.1.1.jar \
    && mv json-smart-1.1.1.jar $HIVE_HOME/lib

# Apache Atlas HiveHook installation
ENV ATLAS_VERSION=2.0.0
ENV HBASE_VERSION=2.0.2
ENV JACKSON_VERSION=2.9.9
ENV JERSEY_VERSION=1.19
ENV JSR311_VERSION=1.1
ENV KAFKA_2_1_1_VERSION=2.0.0
ENV SCALA_LIBRARY_VERSION=2.11.12
ENV COMMONS_CONFIG_VERSION=1.10

RUN mkdir -p $ATLAS_HOME/hook/hive/atlas-hive-plugin-impl

RUN wget -P ${ATLAS_HOME}/hook/hive/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-plugin-classloader/$ATLAS_VERSION/atlas-plugin-classloader-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/ https://repo1.maven.org/maven2/org/apache/atlas/hive-bridge-shim/$ATLAS_VERSION/hive-bridge-shim-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-common/$ATLAS_VERSION/atlas-client-common-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-v1/$ATLAS_VERSION/atlas-client-v1-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-v2/$ATLAS_VERSION/atlas-client-v2-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-common/$ATLAS_VERSION/atlas-common-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-intg/$ATLAS_VERSION/atlas-intg-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/atlas-notification/$ATLAS_VERSION/atlas-notification-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/hdfs-model/$ATLAS_VERSION/hdfs-model-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/hive-bridge/$ATLAS_VERSION/hive-bridge-$ATLAS_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/hbase/hbase-common/$HBASE_VERSION/hbase-common-$HBASE_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/hbase/hbase-server/$HBASE_VERSION/hbase-server-$HBASE_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/$JACKSON_VERSION/jackson-annotations-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/$JACKSON_VERSION/jackson-core-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/$JACKSON_VERSION/jackson-databind-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-jaxb-annotations/$JACKSON_VERSION/jackson-module-jaxb-annotations-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-base/$JACKSON_VERSION/jackson-jaxrs-base-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-json-provider/$JACKSON_VERSION/jackson-jaxrs-json-provider-$JACKSON_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/sun/jersey/jersey-json/$JERSEY_VERSION/jersey-json-$JERSEY_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/com/sun/jersey/contribs/jersey-multipart/$JERSEY_VERSION/jersey-multipart-$JERSEY_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/$KAFKA_2_1_1_VERSION/kafka-clients-$KAFKA_2_1_1_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/$KAFKA_2_1_1_VERSION/kafka_2.12-$KAFKA_2_1_1_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/scala-lang/scala-library/$SCALA_LIBRARY_VERSION/scala-library-$SCALA_LIBRARY_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/commons-configuration/commons-configuration/$COMMONS_CONFIG_VERSION/commons-configuration-$COMMONS_CONFIG_VERSION.jar
RUN wget -P ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/ https://repo1.maven.org/maven2/org/apache/atlas/hdfs-model/$ATLAS_VERSION/hdfs-model-$ATLAS_VERSION.jar


ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.5.0/wait /wait
RUN chmod +x /wait

COPY init-hive.sh /
COPY start-hive.sh /
COPY run_hive_import.sh /
COPY health-check.sh /
COPY log4j.json.properties .

RUN mkdir -p $ATLAS_HOME/hook-bin/
COPY atlas/import_hive.sh $ATLAS_HOME/hook-bin/
RUN chmod +x /$ATLAS_HOME/hook-bin/import_hive.sh

CMD /wait && /start-hive.sh
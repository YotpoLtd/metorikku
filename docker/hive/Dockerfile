FROM openjdk:8u332-jre-slim-bullseye

RUN mkdir /opt/atlas
ENV ATLAS_HOME=/opt/atlas
RUN mkdir -p $ATLAS_HOME/hook/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=2.7.4
RUN apt-get update && apt-get install -y ant && apt-get install -y expect

ADD https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz /hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xzf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV HIVE_HOME=/opt/hive
ARG HIVE_VERSION=2.3.7
ADD https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz /apache-hive-$HIVE_VERSION-bin.tar.gz
RUN tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

ENV MYSQL_CONNECTOR_VERSION=5.1.47
ADD https://repo1.maven.org/maven2/mysql/mysql-connector-java/$MYSQL_CONNECTOR_VERSION/mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar \
    $HIVE_HOME/lib/mysql-connector-java-$MYSQL_CONNECTOR_VERSION.jar

ARG HUDI_VERSION=0.10.0
ADD https://repo1.maven.org/maven2/org/apache/hudi/hudi-hive-sync-bundle/$HUDI_VERSION/hudi-hive-sync-bundle-$HUDI_VERSION.jar \
    $HIVE_HOME/lib/hudi-hive-sync-bundle-$HUDI_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/$HUDI_VERSION/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar \
    $HIVE_HOME/lib/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar \
    $HIVE_HOME/lib/hadoop-aws-$HADOOP_VERSION.jar

ENV AWS_JAVA_SDK_VERSION=1.7.4
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/$AWS_JAVA_SDK_VERSION/aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar \
    $HIVE_HOME/lib/aws-java-sdk-$AWS_JAVA_SDK_VERSION.jar

# Apiary Kafka Metastore Listener Installation
ENV APIARY_EXTENSIONS_VERSION 7.2.0
ENV KAFKA_VERSION 2.6.0
ADD https://repo1.maven.org/maven2/com/expediagroup/apiary/kafka-metastore-listener/${APIARY_EXTENSIONS_VERSION}/kafka-metastore-listener-${APIARY_EXTENSIONS_VERSION}-all.jar $HIVE_HOME/lib/kafka-metastore-listener-${APIARY_EXTENSIONS_VERSION}-all.jar
ADD https://repo1.maven.org/maven2/com/expediagroup/apiary/apiary-hive-events/${APIARY_EXTENSIONS_VERSION}/apiary-hive-events-${APIARY_EXTENSIONS_VERSION}.jar $HIVE_HOME/lib/apiary-hive-events-${APIARY_EXTENSIONS_VERSION}.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar $HIVE_HOME/lib/kafka-clients-${KAFKA_VERSION}.jar

# Apache Atlas HiveHook Installation
ENV ATLAS_VERSION=2.0.0
ENV HBASE_VERSION=2.0.2
ENV JACKSON_VERSION=2.9.9
ENV JERSEY_VERSION=1.19
ENV JSR311_VERSION=1.1
ENV SCALA_LIBRARY_VERSION=2.11.12
ENV COMMONS_CONFIG_VERSION=1.10

RUN mkdir -p $ATLAS_HOME/hook/hive/atlas-hive-plugin-impl

ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-plugin-classloader/$ATLAS_VERSION/atlas-plugin-classloader-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-plugin-classloader-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/hive-bridge-shim/$ATLAS_VERSION/hive-bridge-shim-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/hive-bridge-shim-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-common/$ATLAS_VERSION/atlas-client-common-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-client-common-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-v1/$ATLAS_VERSION/atlas-client-v1-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-client-v1-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-client-v2/$ATLAS_VERSION/atlas-client-v2-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-client-v2-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-common/$ATLAS_VERSION/atlas-common-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-common-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-intg/$ATLAS_VERSION/atlas-intg-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-intg-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/atlas-notification/$ATLAS_VERSION/atlas-notification-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/atlas-notification-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/hdfs-model/$ATLAS_VERSION/hdfs-model-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/hdfs-model-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/hive-bridge/$ATLAS_VERSION/hive-bridge-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/hive-bridge-$ATLAS_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/hbase/hbase-common/$HBASE_VERSION/hbase-common-$HBASE_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/hbase-common-$HBASE_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/hbase/hbase-server/$HBASE_VERSION/hbase-server-$HBASE_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/hbase-server-$HBASE_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/$JACKSON_VERSION/jackson-annotations-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-annotations-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/$JACKSON_VERSION/jackson-core-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-core-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/$JACKSON_VERSION/jackson-databind-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-databind-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/module/jackson-module-jaxb-annotations/$JACKSON_VERSION/jackson-module-jaxb-annotations-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-module-jaxb-annotations-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-base/$JACKSON_VERSION/jackson-jaxrs-base-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-jaxrs-base-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/fasterxml/jackson/jaxrs/jackson-jaxrs-json-provider/$JACKSON_VERSION/jackson-jaxrs-json-provider-$JACKSON_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jackson-jaxrs-json-provider-$JACKSON_VERSION.jar
ADD https://repo1.maven.org/maven2/com/sun/jersey/jersey-json/$JERSEY_VERSION/jersey-json-$JERSEY_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jersey-json-$JERSEY_VERSION.jar
ADD https://repo1.maven.org/maven2/com/sun/jersey/contribs/jersey-multipart/$JERSEY_VERSION/jersey-multipart-$JERSEY_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/jersey-multipart-$JERSEY_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/$KAFKA_VERSION/kafka-clients-$KAFKA_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/kafka-clients-$KAFKA_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/$KAFKA_VERSION/kafka_2.12-$KAFKA_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/kafka_2.12-$KAFKA_VERSION.jar
ADD https://repo1.maven.org/maven2/org/scala-lang/scala-library/$SCALA_LIBRARY_VERSION/scala-library-$SCALA_LIBRARY_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/scala-library-$SCALA_LIBRARY_VERSION.jar
ADD https://repo1.maven.org/maven2/commons-configuration/commons-configuration/$COMMONS_CONFIG_VERSION/commons-configuration-$COMMONS_CONFIG_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/commons-configuration-$COMMONS_CONFIG_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/atlas/hdfs-model/$ATLAS_VERSION/hdfs-model-$ATLAS_VERSION.jar ${ATLAS_HOME}/hook/hive/atlas-hive-plugin-impl/hdfs-model-$ATLAS_VERSION.jar

ENV LOG4J_VERSION=2.17.1

RUN rm $HIVE_HOME/lib/log4j-api-*.jar
RUN rm $HIVE_HOME/lib/log4j-core-*.jar
RUN rm $HIVE_HOME/lib/log4j-jul-*.jar
RUN rm $HIVE_HOME/lib/log4j-slf4j-impl-*.jar
RUN rm $HIVE_HOME/lib/log4j-web-*.jar

ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/$LOG4J_VERSION/log4j-api-$LOG4J_VERSION.jar $HIVE_HOME/lib/log4j-api-$LOG4J_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/$LOG4J_VERSION/log4j-core-$LOG4J_VERSION.jar $HIVE_HOME/lib/log4j-core-$LOG4J_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/$LOG4J_VERSION/log4j-slf4j-impl-$LOG4J_VERSION.jar $HIVE_HOME/lib/log4j-slf4j-impl-$LOG4J_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-web/$LOG4J_VERSION/log4j-web-$LOG4J_VERSION.jar $HIVE_HOME/lib/log4j-web-$LOG4J_VERSION.jar
ADD https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-jul/$LOG4J_VERSION/log4j-jul-$LOG4J_VERSION.jar $HIVE_HOME/lib/log4j-jul-$LOG4J_VERSION.jar

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.5.0/wait /wait
RUN chmod +x /wait

ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar /prometheus/
RUN chmod 644 /prometheus/*.jar

COPY init-hive.sh /
COPY start-*.sh /
COPY run_hive_import.sh /
COPY health-check.sh /
COPY log4j2.json.properties .
COPY jmx_config.yml .


RUN mkdir -p $ATLAS_HOME/hook-bin/
COPY atlas/import_hive.sh $ATLAS_HOME/hook-bin/
RUN chmod +x /$ATLAS_HOME/hook-bin/import_hive.sh

CMD /wait && /start-hive.sh

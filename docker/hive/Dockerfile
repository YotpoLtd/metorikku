FROM openjdk:8u212-b04-jre-stretch

RUN mkdir /opt/atlas
ENV ATLAS_HOME=/opt/atlas
RUN mkdir -p $ATLAS_HOME/hook/hive
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_VERSION=2.8.0
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


ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$HADOOP_VERSION/hadoop-aws-$HADOOP_VERSION.jar \
    $HIVE_HOME/lib/hadoop-aws-$HADOOP_VERSION.jar

ENV AWS_JAVA_SDK_VERSION=1.11.271
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$AWS_JAVA_SDK_VERSION/aws-java-sdk-bundle-$AWS_JAVA_SDK_VERSION.jar \
    $HIVE_HOME/lib/aws-java-sdk-budle-$AWS_JAVA_SDK_VERSION.jar
    
ENV LOG4J_VERSION=2.17.0

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
COPY start-hive.sh /
COPY run_hive_import.sh /
COPY health-check.sh /
COPY log4j2.json.properties .
COPY jmx_config.yml .

CMD /wait && /start-hive.sh

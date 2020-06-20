ARG IMAGE_NAME=metorikku/spark:k8s
FROM ${IMAGE_NAME}

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
ARG HIVE_VERSION=2.3.3
ENV HIVE_VERSION=${HIVE_VERSION}
RUN wget -q https://archive.apache.org/dist/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& tar -xzf apache-hive-$HIVE_VERSION-bin.tar.gz \
	&& mv apache-hive-$HIVE_VERSION-bin $HIVE_HOME \
	&& rm apache-hive-$HIVE_VERSION-bin.tar.gz

#Hudi for hive
ENV HUDI_VERSION=0.5.3
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hive-bundle/$HUDI_VERSION/hudi-hive-bundle-$HUDI_VERSION.jar \
    && mv hudi-hive-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/$HUDI_VERSION/hudi-hadoop-mr-bundle-$HUDI_VERSION.jar \
    && mv hudi-hadoop-mr-bundle-$HUDI_VERSION.jar $HIVE_HOME/lib
RUN wget -q https://repo1.maven.org/maven2/com/tdunning/json/1.8/json-1.8.jar -P ${SPARK_HOME}/jars/

ENV PATH="${HADOOP_PREFIX}/bin:${SPARK_HOME}/bin:${PATH}"
ADD spark-env.sh ${SPARK_HOME}/conf/
ENV USE_BUILTIN_HIVE_METASTORE=false

ENV SPARK_DIST_CLASSPATH /etc/hadoop:/opt/hadoop-2.9.2/share/hadoop/common/lib/*:/opt/hadoop-2.9.2/share/hadoop/common/*:/opt/hadoop-2.9.2/share/hadoop/hdfs:/opt/hadoop-2.9.2/share/hadoop/hdfs/lib/*:/opt/hadoop-2.9.2/share/hadoop/hdfs/*:/opt/hadoop-2.9.2/share/hadoop/yarn:/opt/hadoop-2.9.2/share/hadoop/yarn/lib/*:/opt/hadoop-2.9.2/share/hadoop/yarn/*:/opt/hadoop-2.9.2/share/hadoop/mapreduce/lib/*:/opt/hadoop-2.9.2/share/hadoop/mapreduce/*:/contrib/capacity-scheduler/*.jar:${SPARK_HOME}/custom/conf/
ENV SPARK_EXTRA_CLASSPATH $SPARK_DIST_CLASSPATH

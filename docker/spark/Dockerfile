FROM openjdk:8-jre

# BASE
ARG SPARK_VERSION
ARG HADOOP_VERSION

RUN wget -q http://apache.mirror.iphh.net/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && cd /

RUN apt-get update \
    && apt-get install -y jq \
    && apt-get install -y python3 python3-setuptools \
    && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python3 get-pip.py 'pip==9.0.1' \
    && rm get-pip.py

ENV PYTHONHASHSEED 1
ENV SPARK_HOME /spark
ENV PATH="/spark/bin:${PATH}"

COPY entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
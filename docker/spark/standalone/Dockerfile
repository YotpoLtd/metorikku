ARG IMAGE_NAME=metorikku/spark:k8s
FROM ${IMAGE_NAME}

RUN rm -f ${SPARK_HOME}/jars/spark-bigquery-latest.jar

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

ADD conf/* ${SPARK_HOME}/conf/
ADD scripts /scripts

ENTRYPOINT ["/scripts/entrypoint-master.sh"]
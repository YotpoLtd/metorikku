ARG IMAGE_NAME=metorikku/spark:k8s
FROM ${IMAGE_NAME}

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

ADD conf/* ${SPARK_HOME}/conf/
ADD scripts /scripts

ENTRYPOINT ["/scripts/entrypoint-master.sh"]
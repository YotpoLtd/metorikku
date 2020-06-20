ARG IMAGE_NAME=metorikku/spark-base
FROM ${IMAGE_NAME}

ADD spark-defaults.conf ${SPARK_HOME}/custom/conf/
ADD scripts /scripts

ENTRYPOINT ["/scripts/entrypoint-master.sh"]
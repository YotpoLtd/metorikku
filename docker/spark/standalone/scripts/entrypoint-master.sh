#!/bin/bash

# Default values
SPARK_MASTER_PORT=${SPARK_MASTER_PORT:=7077}
SPARK_WEBUI_PORT=${SPARK_WEBUI_PORT:=8080}
SPARK_MASTER_HOST=${SPARK_MASTER_HOST:=spark-master}

# Logs
/scripts/init-logs-metrics.sh

. "${SPARK_HOME}/sbin/spark-config.sh"
. "${SPARK_HOME}/bin/load-spark-env.sh"
${SPARK_HOME}/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip ${SPARK_MASTER_HOST} --port ${SPARK_MASTER_PORT} --webui-port ${SPARK_WEBUI_PORT}

#!/bin/bash

# Logs
/init-logs.sh

. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"
/spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip ${SPARK_MASTER_HOST} --port ${SPARK_MASTER_PORT} --webui-port ${SPARK_WEBUI_PORT}

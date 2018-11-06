#!/bin/bash

# Logs
/scripts/init-logs-metrics.sh

# Monitor Logs
/scripts/monitor-executor-logs.sh &

. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"
SPARK_MASTER="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port ${SPARK_WORKER_WEBUI_PORT} --port ${SPARK_WORKER_PORT} ${SPARK_MASTER}

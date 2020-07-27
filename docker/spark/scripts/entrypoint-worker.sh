#!/bin/bash

# Default values
SPARK_MASTER_PORT=${SPARK_MASTER_PORT:=7077}
SPARK_WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:=8081}
SPARK_MASTER_HOST=${SPARK_MASTER_HOST:=spark-master}
SPARK_WORKER_PORT=${SPARK_WORKER_PORT:=7078}
SHUFFLE_SERVICE_PORT=${SHUFFLE_SERVICE_PORT:=7337}
SPARK_WORKER_OPTS=${SPARK_WORKER_OPTS:="-Dspark.shuffle.service.enabled=true -Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.appDataTtl=1800"}

# Logs
/scripts/init-logs-metrics.sh

# Monitor Logs
/scripts/monitor-executor-logs.sh &

echo -e "
spark.shuffle.service.port=$SHUFFLE_SERVICE_PORT
" >> /spark/conf/spark-defaults.conf

. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"
SPARK_MASTER="spark://$SPARK_MASTER_HOST:$SPARK_MASTER_PORT"
/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port ${SPARK_WORKER_WEBUI_PORT} --port ${SPARK_WORKER_PORT} --properties-file /spark/conf/spark-defaults.conf ${SPARK_MASTER}

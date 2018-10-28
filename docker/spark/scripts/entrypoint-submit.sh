#!/bin/bash

# Logs
/scripts/init-logs-metrics.sh

# Wait until cluster is up
URL="http://${SPARK_MASTER_HOST}:${SPARK_WEBUI_PORT}"
MAX_RETRIES=${MAX_RETRIES:=300}
NUM_WORKERS=${NUM_WORKERS:=1}
active_workers=0
until [[ ${active_workers} -eq ${NUM_WORKERS} ]] || [[ ${MAX_RETRIES} -eq 0 ]] ; do
    sleep 1s
    active_workers=`curl -s ${URL}/json/ | jq '.workers | length'`
    ((MAX_RETRIES--))
done

if [[ ${MAX_RETRIES} -eq 0 ]] ; then
    echo "Cluster $URL is not ready - stopping"
    exit 1
fi

# Run command
SPARK_MASTER="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
SPARK_UI_PORT=${SPARK_UI_PORT:="4040"}
echo -e "
spark.master $SPARK_MASTER
spark.ui.port $SPARK_UI_PORT
" >> /spark/conf/spark-defaults.conf

echo "Running command: ${SUBMIT_COMMAND}"
${SUBMIT_COMMAND}

/scripts/finish-submit.sh

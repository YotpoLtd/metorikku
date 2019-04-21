#!/bin/bash

# Default values
SPARK_MASTER_PORT=${SPARK_MASTER_PORT:=7077}
SPARK_WEBUI_PORT=${SPARK_WEBUI_PORT:=8080}
SPARK_MASTER_HOST=${SPARK_MASTER_HOST:=spark-master}
MAX_RETRIES=${MAX_RETRIES:=300}
MAX_RETRIES_JOB=${MAX_RETRIES_JOB:=1}
MIN_WORKERS=${MIN_WORKERS:=1}
SPARK_UI_PORT=${SPARK_UI_PORT:=4040}

# Logs
/scripts/init-logs-metrics.sh

# Wait until cluster is up
URL="http://${SPARK_MASTER_HOST}:${SPARK_WEBUI_PORT}"

active_workers=0
echo "Checking if master ${URL} have minimum workers ${MIN_WORKERS}"
until [[ ${active_workers} -ge ${MIN_WORKERS} ]] || [[ ${MAX_RETRIES} -eq 0 ]] ; do
    sleep 1s
    active_workers=`curl --connect-timeout 10 --max-time 10 -s ${URL}/json/ | jq '.aliveworkers'`
    echo "waiting for ${URL}, to have minimum workers: ${MIN_WORKERS}, active workers: ${active_workers}"
    ((MAX_RETRIES--))
done

if [[ ${MAX_RETRIES} -eq 0 ]] ; then
    echo "Cluster $URL is not ready - stopping"
    exit 1
fi

# Run command
SPARK_MASTER="spark://${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT}"
echo -e "
spark.master $SPARK_MASTER
spark.ui.port $SPARK_UI_PORT
" >> /spark/conf/spark-defaults.conf

echo "Running command: ${SUBMIT_COMMAND}"

NEXT_WAIT_TIME=0
EXIT_CODE=1

until [[ ${EXIT_CODE} -eq 0 ]] || [[ ${NEXT_WAIT_TIME} -eq ${MAX_RETRIES_JOB} ]]; do
   ${SUBMIT_COMMAND}
   EXIT_CODE=$?
   sleep $(( NEXT_WAIT_TIME++ ))
   echo "Attempt ${NEXT_WAIT_TIME} ..."
done

/scripts/finish-submit.sh

exit ${EXIT_CODE}

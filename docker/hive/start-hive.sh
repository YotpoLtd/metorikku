#!/bin/bash
# Init Hive
./init-hive.sh

echo "Start Hive"
METASTORE_PORT=${METASTORE_PORT:=9083}

nohup ${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT} &
sleep 10s
${HIVE_HOME}/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console

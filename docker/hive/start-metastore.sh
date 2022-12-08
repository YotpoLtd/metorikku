#!/bin/bash

if [[ -z "$1" ]]
then
# Init Hive
./init-hive.sh

echo "Start MetaStore"
METASTORE_PORT=${METASTORE_PORT:=9083}
JMX_PORT=${JMX_PORT:=8008}
JMX_PROMETHEUS_PORT=${JMX_PROMETHEUS_PORT:=8080}

${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT}

else
nohup ${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT} &
fi


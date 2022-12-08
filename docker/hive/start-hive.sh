#!/bin/bash
# Init Hive
./init-hive.sh

echo "Start Hive"
METASTORE_PORT=${METASTORE_PORT:=9083}
JMX_PORT=${JMX_PORT:=8008}
JMX_PROMETHEUS_PORT=${JMX_PROMETHEUS_PORT:=8080}
PROFILE=$1

. ./start-metastore.sh "skip-init"
. ./start-hiveserver.sh "skip-init"

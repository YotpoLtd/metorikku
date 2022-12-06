#!/bin/bash
# Init Hive
./init-hive.sh

echo "Start Hive"
METASTORE_PORT=${METASTORE_PORT:=9083}
JMX_PORT=${JMX_PORT:=8008}
JMX_PROMETHEUS_PORT=${JMX_PROMETHEUS_PORT:=8080}
PROFILE=$1

if [[ $PROFILE -eq "metastore" ]]
then
nohup ${HIVE_HOME}/bin/hive --service metastore -p ${METASTORE_PORT} &
else
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Dcom.sun.management.jmxremote.port=${JMX_PORT} -javaagent:/prometheus/jmx_prometheus_javaagent-0.11.0.jar=${JMX_PROMETHEUS_PORT}:/jmx_config.yml"
${HIVE_HOME}/bin/hiveserver2 --hiveconf hive.root.logger=INFO,console
fi
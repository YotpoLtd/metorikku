#!/bin/bash
# Atlas application properties
USE_ATLAS=${USE_ATLAS:=false}
if [[ "${USE_ATLAS}" == true ]] ; then
echo -e "
atlas.kafka.zookeeper.connect=${ATLAS_ZOOKEEPER_CONNECT}
atlas.kafka.bootstrap.servers=${ATLAS_BOOTSTRAP_SERVERS}
" >> ${SPARK_HOME}/conf/atlas-application.properties

echo -e "
spark.extraListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.queryExecutionListeners=com.hortonworks.spark.atlas.SparkAtlasEventTracker
spark.sql.streaming.streamingQueryListeners=com.hortonworks.spark.atlas.SparkAtlasStreamingQueryEventTracker
" >> ${SPARK_HOME}/conf/spark-defaults.conf

fi
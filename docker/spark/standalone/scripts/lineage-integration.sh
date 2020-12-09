#!/bin/bash

USE_LINEAGE=${USE_LINEAGE:=false}
if [[ "${USE_LINEAGE}" == true ]] ; then

echo -e "
spark.sql.queryExecutionListeners=org.chojin.spark.lineage.SparkSqlLineageListener
" >> ${SPARK_HOME}/conf/spark-defaults.conf

fi

echo -e "
org.chojin.spark.lineage.reporters=org.chojin.spark.lineage.reporter.KafkaReporter
org.chojin.spark.lineage.reporter.kafka.bootstrapServerUrl=${LINEAGE_BOOTSTRAP_SERVERS}
org.chojin.spark.lineage.reporter.kafka.topic=${LINEAGE_TOPIC:=spark_lineage}
" >> ${SPARK_HOME}/conf/lineage.properties

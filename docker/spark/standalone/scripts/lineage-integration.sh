#!/bin/bash

USE_LINEAGE=${USE_LINEAGE:=false}
if [[ "${USE_LINEAGE}" == true ]] ; then

echo -e "
spark.sql.queryExecutionListeners=org.chojin.spark.lineage.SparkSqlLineageListener
" >> ${SPARK_HOME}/conf/spark-defaults.conf

fi

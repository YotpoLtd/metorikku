#!/bin/bash
JSON_LOG_LEVEL=${JSON_LOG_LEVEL:=WARN}

if [ ! -z ${GRAPHITE_HOST} ] ; then
    echo "Using graphite host $GRAPHITE_HOST"
    echo -e "
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=$GRAPHITE_HOST
*.sink.graphite.period=10
*.sink.graphite.port=$GRAPHITE_PORT
" >> ${SPARK_HOME}/conf/metrics.properties
fi

if [ ! -z ${JSON_LOG} ] ; then
    echo "Setting Log type to JSON"
    eval "echo \"$(< ${SPARK_HOME}/conf/log4j.json.properties)\"" >> ${SPARK_HOME}/conf/log4j.properties
else
    eval "echo \"$(< ${SPARK_HOME}/conf/log4j.properties.template)\"" >> ${SPARK_HOME}/conf/log4j.properties
fi

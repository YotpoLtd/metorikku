#!/bin/bash

if [ ! -z ${GRAPHITE_HOST} ] ; then
    echo "Using graphite host $GRAPHITE_HOST"
    echo -e "
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=$GRAPHITE_HOST
*.sink.graphite.period=10
*.sink.graphite.port=$GRAPHITE_PORT
" >> /spark/conf/metrics.properties
fi

if [ ! -z ${JSON_LOG} ] ; then
    echo "Setting Log type to JSON"
    cat /spark/conf/log4j.json.properties >> /spark/conf/log4j.properties
fi

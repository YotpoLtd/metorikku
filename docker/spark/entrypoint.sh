#!/bin/bash
. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"

if [ ! -z "$GRAPHITE_HOST" ] ; then
    echo "Using graphite host $GRAPHITE_HOST"
    echo -e "
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=$GRAPHITE_HOST
*.sink.graphite.period=10
*.sink.graphite.port=$GRAPHITE_PORT
" >> /spark/conf/metrics.properties
fi

if [ ! -z "$JSON_LOG" ] ; then
    echo "Setting Log type to JSON"
    cat /spark/conf/log4j.json.properties >> /spark/conf/log4j.properties
fi

if [ $SPARK_NODE = "master" ] ; then
	/spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
	    --ip $SPARK_HOST --port $SPARK_PORT --webui-port $SPARK_WEBUI_PORT &

	active_workers=0
	url="http://$SPARK_HOST:$SPARK_WEBUI_PORT"
	MAX_RETRIES=${MAX_RETRIES:=300}
	NUM_WORKERS=${NUM_WORKERS:=1}
	until [[ ${active_workers} -eq ${NUM_WORKERS} ]] || [[ ${MAX_RETRIES} -eq 0 ]] ; do
		sleep 1s
		echo "Trying to connect $url, waiting for $MAX_RETRIES seconds more"
		active_workers=`curl -s ${url}/json/ | jq '.workers | length'`
		((MAX_RETRIES--))
	done

	if [[ ${MAX_RETRIES} -eq 0 ]] ; then
		echo "Unable to connect to server - stopping"
		exit 1
	else
	    SPARK_UI_PORT=${SPARK_UI_PORT:="4040"}
		echo -e "
spark.master spark://$SPARK_HOST:$SPARK_PORT
spark.ui.port $SPARK_UI_PORT
" >> /spark/conf/spark-defaults.conf
	fi

	echo "Running command $SUBMIT_COMMAND"
	$SUBMIT_COMMAND
else
	/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
	    --webui-port $SPARK_WEBUI_PORT --port $SPARK_PORT $SPARK_MASTER
fi

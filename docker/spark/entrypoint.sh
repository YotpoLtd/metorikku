#!/bin/bash
. "/spark/sbin/spark-config.sh"
. "/spark/bin/load-spark-env.sh"

if [ -z $SPARK_NODE ] \
    || [ -z $SPARK_HOST ] \
    || [ -z $SPARK_PORT ] \
    || [ -z $SPARK_WEBUI_PORT ] \
    || [ -z $SPARK_MASTER ] \
    || [ -z $SUBMIT_COMMAND ] ; then
    echo "Missing args - stopping"
    exit 1
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
		echo -e "\nspark.master spark://$SPARK_HOST:$SPARK_PORT\n" >> /spark/conf/spark-defaults.conf
		echo "Running command $SUBMIT_COMMAND"
		$SUBMIT_COMMAND
	fi

else
	/spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
	    --webui-port $SPARK_WEBUI_PORT --port $SPARK_PORT $SPARK_MASTER
fi

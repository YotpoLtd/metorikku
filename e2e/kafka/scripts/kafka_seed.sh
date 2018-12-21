#!/bin/bash

/scripts/wait_for_topic.sh

kafka-console-producer.sh --request-required-acks 1 --broker-list kafka:9092 --topic ${TOPIC} < ${SEED_FILE}
echo "Completed producing see to kafka topic ${TOPIC}"

if [ ! -z ${SLEEP_FOREVER} ] ; then
    while true; do sleep 86400; done
fi

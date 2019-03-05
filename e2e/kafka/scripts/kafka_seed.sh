#!/bin/bash

KAFKA_BROKER=${KAFKA_BROKER:=kafka:9092}

/scripts/wait_for_topic.sh

kafka-console-producer.sh --request-required-acks 1 --broker-list ${KAFKA_BROKER} --topic ${TOPIC} < ${SEED_FILE}
echo "Completed producing seed to kafka topic ${TOPIC}"

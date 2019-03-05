#!/bin/bash

ZOOKEEPER=${ZOOKEEPER:=zookeeper:2181}

until [[ $(kafka-topics.sh --describe --topic ${TOPIC} --zookeeper ${ZOOKEEPER}) ]];
do
  echo "Waiting for Kafka topic ${TOPIC} to be ready..."
  sleep 2s
done
echo "Kafka topic ${TOPIC} is ready"

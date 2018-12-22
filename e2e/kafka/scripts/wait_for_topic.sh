#!/bin/bash

ZOOKEEPER=${ZOOKEEPER:=zookeeper:2181}

until [[ $(sleep 2s && kafka-topics.sh --describe --topic ${TOPIC} --zookeeper ${ZOOKEEPER}) ]];
do
  echo "Waiting for Kafka topic ${TOPIC} to be ready..."
done
echo "Kafka topic ${TOPIC} is ready"

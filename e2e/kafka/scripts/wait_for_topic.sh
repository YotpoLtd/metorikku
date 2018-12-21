#!/bin/bash

until [[ $(sleep 2s && kafka-topics.sh --describe --topic ${TOPIC} --zookeeper zookeeper:2181) ]];
do
  echo "Waiting for Kafka topic ${TOPIC} to be ready..."
done
echo "Kafka topic ${TOPIC} is ready"

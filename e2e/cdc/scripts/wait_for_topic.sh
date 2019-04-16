#!/bin/bash
kafka-topics.sh  --list --bootstrap-server ${KAFKA_BROKER}
until [[ $(sleep 2s && kafka-topics.sh --describe --topic ${TOPIC} --zookeeper ${ZOOKEEPER}) ]];
do
  echo "Waiting for Kafka topic ${TOPIC} to be ready..."
done
echo "Kafka topic ${TOPIC} is ready"

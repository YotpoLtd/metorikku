#!/bin/bash

KAFKA_BROKER=${KAFKA_BROKER:=kafka:9092}
KAFKA_CONSUME_TIMEOUT=${KAFKA_CONSUME_TIMEOUT:=120000}

/scripts/wait_for_topic.sh

set -e

kafka-console-consumer.sh --bootstrap-server ${KAFKA_BROKER} --from-beginning --topic ${TOPIC} --max-messages ${NUMBER_OF_EXPECTED_MESSAGES} --timeout-ms ${KAFKA_CONSUME_TIMEOUT} > /tmp/test_results
echo "completed consuming from topic ${TOPIC}, results:"
cat /tmp/test_results

diff -w <(sort ${MOCK_OUTPUT}) <(sort /tmp/test_results)

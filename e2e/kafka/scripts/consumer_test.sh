#!/bin/bash

/scripts/wait_for_topic.sh

set -e

kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --topic ${TOPIC} --max-messages ${NUMBER_OF_EXPECTED_MESSAGES} --timeout-ms 120000 > /tmp/test_results.jsonl
diff -w <(sort ${MOCK_OUTPUT}) <(sort /tmp/test_results.jsonl)

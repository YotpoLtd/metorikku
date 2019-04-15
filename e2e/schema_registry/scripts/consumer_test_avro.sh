#!/bin/bash
/scripts/wait_for_topic.sh

set -e

kafka-console-consumer.sh \
    --bootstrap-server ${KAFKA_BROKER} \
    --from-beginning \
    --topic ${TOPIC} \
    --max-messages ${NUMBER_OF_EXPECTED_MESSAGES} \
    --timeout-ms ${KAFKA_CONSUME_TIMEOUT} > /tmp/test_results

echo "completed consuming from topic ${TOPIC}, results:"
cat /tmp/test_results
echo "starting to compare expected results:"
cat ${MOCK_OUTPUT}

jq 'del(.ts_ms)'  /tmp/test_results > /tmp/test_results_without_ms
jq 'del(.. | .source?)' /tmp/test_results_without_ms > /tmp/test_results_without_ms_sec

jq 'del(.ts_ms)' ${MOCK_OUTPUT} > /tmp/expected_without_ms
jq 'del(.. | .source?)' /tmp/expected_without_ms > /tmp/expected_without_ms_sec


diff -w <(sort /tmp/expected_without_ms_sec) <(sort /tmp/test_results_without_ms_sec)

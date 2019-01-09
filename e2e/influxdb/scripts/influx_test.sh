#!/bin/bash

INFLUXDB=${INFLUXDB:=localhost:8086}
DB=${DB:=test}

/scripts/wait_for_influx.sh

set -e

runQuery() {
  curl \
    -u user:pass \
    "http://${INFLUXDB}/query?db=${DB}&pretty=true" \
    --data-urlencode "q=${QUERY}"
}

query=$(runQuery)
until [ $(echo "$query" | wc -l) = 2960 ]; do
  sleep 2s
  echo "Waiting for influxdb to be ready..."
  urlstatus=$(fetchstatus)
done
echo "influxDB is ready"

curl -u user:pass "http://${INFLUXDB}/query?db=${DB}&pretty=true" --data-urlencode "q=${QUERY}" > /tmp/test_results
echo "completed consuming from topic ${TOPIC}, results:"

cat


kafka-console-consumer.sh --bootstrap-server ${KAFKA_BROKER} --from-beginning --topic ${TOPIC} --max-messages ${NUMBER_OF_EXPECTED_MESSAGES} --timeout-ms ${KAFKA_CONSUME_TIMEOUT} > /tmp/test_results
echo "completed consuming from topic ${TOPIC}, results:"
cat /tmp/test_results

diff -w <(sort ${MOCK_OUTPUT}) <(sort /tmp/test_results)

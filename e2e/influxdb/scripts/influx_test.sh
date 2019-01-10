#!/bin/bash
INFLUXDB=${INFLUXDB:=influxdb:8086}
DB=${DB:=test}
MAX_RETRIES=${MAX_RETRIES:=60}
INFLUX_USER=${INFLUX_USER:=user}
INFLUX_PASSWORD=${INFLUX_PASSOWRD:=pass}

# Wait for influxdb
fetchStatus() {
  curl \
    -o /dev/null \
    --silent \
    --head \
    --write-out '%{http_code}' \
    ${INFLUXDB}/ping
}

urlStatus=$(fetchStatus)
until [[ "$urlStatus" = 204 ]]; do
  sleep 2s
  echo "Waiting for influxdb to be ready..."
  urlStatus=$(fetchStatus)
done
echo "influxDB is ready"

set -e

# Wait for query
runQuery() {
  curl \
    -u ${INFLUX_USER}:${INFLUX_PASSWORD} --silent \
    "${INFLUXDB}/query?db=${DB}&pretty=true" \
    --data-urlencode "q=${QUERY}"
}

num_of_expected_rows=$(cat ${MOCK_OUTPUT} | wc -l)
echo "Waiting for influxdb to respond to query with ${num_of_expected_rows} lines"

results=$(runQuery)
num_retries=0
until [[ $(echo "$results" | wc -l) = ${num_of_expected_rows} || num_retries = ${MAX_RETRIES} ]]; do
  sleep 2s
  if [[ ! -z ${DEBUG} ]]; then
    echo -e "Current results:\n$results"
  fi
  results=$(runQuery)
  num_retries=$((num_retries + 1))
done

if [[ num_retries -eq ${MAX_RETRIES} ]]; then
    echo "Failed to obtain proper results within ${MAX_RETRIES} retries, exiting"
    exit 1
fi
echo "influxDB query matches mock in the number of lines"

# Compare results
echo -e "Results:\n$results"
echo "$results" > /tmp/test_results

diff -w ${MOCK_OUTPUT} /tmp/test_results

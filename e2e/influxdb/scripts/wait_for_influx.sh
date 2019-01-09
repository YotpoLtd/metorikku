#!/bin/bash

fetchstatus() {
  curl \
    -o /dev/null \
    --silent \
    --head \
    --write-out '%{http_code}' \
    ${INFLUXDB}/ping
}

urlstatus=$(fetchstatus)
until [ "$urlstatus" = 204 ]; do
  sleep 2s
  echo "Waiting for influxdb to be ready..."
  urlstatus=$(fetchstatus)
done
echo "influxDB is ready"

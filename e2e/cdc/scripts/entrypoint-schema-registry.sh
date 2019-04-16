#!/bin/bash

# Wait for kafka connect
fetchStatus() {
  curl -o /dev/null \
    --silent \
    --head \
    --write-out '%{http_code}' \
     -H "Accept:application/json" http://connect:8083/connectors/
}

urlStatus=$(fetchStatus)

until [[ "$urlStatus" = 200 ]]; do
  echo $urlStatus
  sleep 2s
  echo "Waiting for kafka connect to be ready..."
  urlStatus=$(fetchStatus)
done
echo "kafka connect is ready"

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://connect:8083/connectors/ -d @/scripts/register-mysql.json

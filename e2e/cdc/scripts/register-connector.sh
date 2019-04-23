#!/bin/bash
CONNECT_HOST=${CONNECT_HOST:=connect}
CONNECT_PORT=${CONNECT_PORT:=8083}
# Wait for kafka connect
fetchStatus() {
  curl -o /dev/null \
    --silent \
    --head \
    --write-out '%{http_code}' \
     -H "Accept:application/json" http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/
}

urlStatus=$(fetchStatus)

until [[ "$urlStatus" = 200 ]]; do
  echo $urlStatus
  sleep 2s
  echo "Waiting for kafka connect to be ready..."
  urlStatus=$(fetchStatus)
done
echo "kafka connect is ready"

/scripts/wait_for_mysql.sh

curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://${CONNECT_HOST}:${CONNECT_PORT}/connectors/ -d @${REGISTER_JSON_PATH}

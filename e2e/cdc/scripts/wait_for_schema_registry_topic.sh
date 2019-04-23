#!/bin/bash

fetchStatus() {
  curl -o /dev/null \
    --silent \
    --head \
    --write-out '%{http_code}' \
     -H "Accept:application/json" ${SCHEMA_REGISTRY_LISTENERS}/subjects/${WAIT_SCHEMA_REGISTRY_SUBJECT}-value/versions/latest
}

echo "Schema Registry Endpoint:"
echo ${SCHEMA_REGISTRY_LISTENERS}/subjects/${WAIT_SCHEMA_REGISTRY_SUBJECT}-value/versions/latest
echo "Current Subjects:"
curl -H "Accept:application/json" ${SCHEMA_REGISTRY_LISTENERS}/subjects/

urlStatus=$(fetchStatus)
until [[ "$urlStatus" = 200 ]]; do
  echo $urlStatus
  sleep 5s
  echo "Waiting for Schema Registry subject to be ready..."
  urlStatus=$(fetchStatus)
done
echo "Schema Registry subject is ready"

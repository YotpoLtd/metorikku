#!/bin/bash
ELASTICSEARCH=${ELASTICSEARCH:=elasticsearch:9200}
INDEX_NAME=${INDEX_NAME:=}

# wait for elastic to be ready
until [ "$(sleep 2s && curl --write-out %{http_code} --silent -S --output /dev/null http://${ELASTICSEARCH}/_cat/health\?h\=st)" -eq "200" ];
do
  echo "Waiting for Elasticsearch to be ready..."
done
echo "Elasticsearch is ready!"

curl -X PUT "http://${ELASTICSEARCH}/${INDEX_NAME}" -H 'Content-Type: application/json' -d'
{
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
  }
}
'

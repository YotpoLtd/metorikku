#!/bin/bash
ELASTICSEARCH=${ELASTICSEARCH:=elasticsearch:9200}
SORTBY=${SORTBY:=movieid}
MAX_RETRIES=${MAX_RETRIES:=60}

# wait for elastic to be ready
until [ "$(sleep 2s && curl --write-out %{http_code} --silent -S --output /dev/null http://${ELASTICSEARCH}/_cat/health\?h\=st)" -eq "200" ];
do
  echo "Waiting for Elasticsearch to be ready..."
done
echo "Elasticsearch is ready!"

curl -X PUT "http://${ELASTICSEARCH}/movies" -H 'Content-Type: application/json' -d'
{
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
  }
}
'

runCountQuery() {
  curl -s -X GET "http://${ELASTICSEARCH}/${INDEX_NAME}/_count" | jq .count
}

runFullQuery() {
    curl -X GET "http://${ELASTICSEARCH}/${INDEX_NAME}/_search" -H 'Content-Type: application/json' -d "{\"sort\": [\"$SORTBY\"]}" | jq .hits.hits
}

countMock() {
    jq length ${MOCK_OUTPUT}
}

num_of_expected_rows=$(countMock)

echo "Waiting for elastic to respond to query with ${num_of_expected_rows} lines"

results=$(runCountQuery)
num_retries=0
until [[ $(echo "$results") = ${num_of_expected_rows} || num_retries = ${MAX_RETRIES} ]]; do
  sleep 2s
  results=$(runCountQuery)
  num_retries=$((num_retries + 1))
  if [[ ! -z ${DEBUG} ]]; then
    echo -e "Current results:\n$results"
  fi
done

if [[ num_retries -eq ${MAX_RETRIES} ]]; then
    echo "Failed to obtain proper results within ${MAX_RETRIES} retries, exiting"
    exit 1
fi
echo "elasticsearch query matches mock in the number of lines"

results=$(runFullQuery)

# Compare results
if [[ ! -z ${DEBUG} ]]; then
    echo -e "Results:\n$results"
fi
echo "$results" > /tmp/test_results

jq -c . ${MOCK_OUTPUT} > /tmp/mock_unpretty
jq -c . /tmp/test_results > /tmp/test_results_unpretty

echo "Comparing mock and test output..."
diff -w /tmp/mock_unpretty /tmp/test_results_unpretty




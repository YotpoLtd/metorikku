#!/bin/bash
ELASTICSEARCH=${ELASTICSEARCH:=elasticsearch:9200}
MAX_RETRIES=${MAX_RETRIES:=60}
INDEX_NAME=${INDEX_NAME:=}
SORTBY=${SORTBY:=}

runQuery() {
    curl -X GET "http://${ELASTICSEARCH}/${INDEX_NAME}/_search" -H 'Content-Type: application/json' -d "{\"sort\": [\"$SORTBY\"]}" | jq '.hits["hits"][]["_source"]'
}

results=$(runQuery)

# Compare results
if [[ ! -z ${DEBUG} ]]; then
    echo -e "Results:\n$results"
fi
echo "$results" > /tmp/test_results

jq -c . ${MOCK_OUTPUT} > /tmp/mock_unpretty
jq -c . /tmp/test_results > /tmp/test_results_unpretty

echo "Comparing mock and test output..."
diff -w /tmp/mock_unpretty /tmp/test_results_unpretty

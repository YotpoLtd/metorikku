#!/bin/bash
DYNAMODB=${DYNAMODB:=dynamodb:8000}
TABLE_NAME=${TABLE_NAME:=movies}

runQuery() {
    curl -X GET "http://${DYNAMODB}/${TABLE_NAME}"
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

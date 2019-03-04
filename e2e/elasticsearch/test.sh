#!/bin/bash

MOCK_OUTPUT=mocks/movies.json

countMock() {
    jq length ${MOCK_OUTPUT}
}
num_of_expected_rows = $(countMock)
echo "NUM_OF_EXPECTED_ROWS IS"
echo "$num_of_expected_rows"
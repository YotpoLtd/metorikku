#!/bin/bash
set -e

docker-compose -f docker-compose.yml up -d dynamodb
docker-compose -f docker-compose.yml up --exit-code-from dynamodb-init dynamodb-init
docker-compose -f docker-compose.yml up --exit-code-from spark-submit spark-submit
docker-compose -f docker-compose.yml up --exit-code-from dynamodb-tester dynamodb-tester
exit_code=$(docker ps -aq -f label=com.docker.compose.project=dynamodb | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)
docker-compose -f docker-compose.yml down
exit $exit_code

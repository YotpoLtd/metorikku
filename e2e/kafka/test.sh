#!/usr/bin/env bash
set -e

docker-compose up -d kafka
docker-compose up --exit-code-from kafka-seed kafka-seed
docker-compose up --exit-code-from kafka-consume spark-submit kafka-consume
exit_code=$(docker ps -aq -f label=com.docker.compose.project=kafka | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}'| sed 's/^137$/0/g' | paste -sd+ - | bc)
docker-compose down
echo "Exiting with $exit_code"
exit $exit_code

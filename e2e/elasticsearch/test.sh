#!/bin/bash
set -e

docker-compose up -d elasticsearch
docker-compose up --exit-code-from elasticsearch-init elasticsearch-init
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from elasticsearch-tester elasticsearch-tester
exit_code=$(docker ps -aq -f label=com.docker.compose.project=elasticsearch | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)
docker-compose down
exit $exit_code


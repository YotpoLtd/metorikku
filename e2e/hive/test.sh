#!/usr/bin/env bash
set -e

docker-compose up -d hive-db
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from hive-tester hive-tester
exit_code=$(docker ps -aq -f label=com.docker.compose.project=hive | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)
docker-compose down
#rm -rf warehouse
exit $exit_code

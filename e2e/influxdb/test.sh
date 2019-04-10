#!/usr/bin/env bash
set -e

docker-compose up -d influxdb
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from influxdb-tester influxdb-tester
exit_code=$(docker ps -aq -f label=com.docker.compose.project=influxdb | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)
docker-compose down
exit $exit_code

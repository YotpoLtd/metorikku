#!/usr/bin/env bash
mkdir output

set -e

docker-compose up -d hive
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from hive-tester hive-tester
docker-compose up --exit-code-from spark-submit-non-partition spark-submit-non-partition
docker-compose up --exit-code-from hive-tester-no-partition hive-tester-no-partition
exit_code=$(docker ps -aq -f label=com.docker.compose.project=hudi | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)
docker-compose down
exit $exit_code

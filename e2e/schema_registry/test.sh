#!/bin/bash
set -e

docker-compose up -d zookeeper
docker-compose up -d kafka kafka
docker-compose up -d mysql mysql
docker-compose up -d connect connect
docker-compose up -d schema-registry schema-registry

docker-compose up -d spark-master spark-master
docker-compose up -d spark-worker spark-worker
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from schema-registry-register schema-registry-register
docker-compose up --exit-code-from mysql-client mysql-client
docker-compose up --exit-code-from kafka-consume-avro kafka-consume-avro
exit_code=$(docker ps -aq -f label=com.docker.compose.project=kafka-consume-avro | xargs -I{} docker inspect {} --format='{{.State.ExitCode}}' | paste -sd+ - | bc)

docker-compose down
exit $exit_code

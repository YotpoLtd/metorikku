#!/bin/bash
mkdir -p /output/hoodie_test.parquet
set -e

docker-compose up -d connect connect
docker-compose up -d hive hive
docker-compose up --exit-code-from schema-registry-register schema-registry-register
docker-compose up --exit-code-from mysql-client mysql-client
sleep 10
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from hive-tester hive-tester

docker-compose down

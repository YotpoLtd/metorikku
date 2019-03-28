#!/usr/bin/env bash
set -e

docker-compose up -d hive
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from hive-tester hive-tester
docker-compose down

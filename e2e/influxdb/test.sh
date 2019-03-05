#!/usr/bin/env bash
set -e

docker-compose up -d influxdb
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from influxdb-tester influxdb-tester
docker-compose down

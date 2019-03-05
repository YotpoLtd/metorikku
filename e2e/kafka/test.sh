#!/usr/bin/env bash
set -e

docker-compose up -d kafka
docker-compose up --exit-code-from kafka-seed kafka-seed
docker-compose up --exit-code-from kafka-consume spark-submit kafka-consume
docker-compose down

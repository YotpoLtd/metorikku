#!/bin/bash
mkdir output
set -e
docker-compose up -d zookeeper zookeeper
docker-compose up -d kafka kafka
docker-compose up --exit-code-from kafka-wait kafka-wait

docker-compose up -d schema-registry schema-registry

docker-compose up -d mysql mysql
docker-compose up -d hive hive

docker-compose up -d connect connect
docker-compose up --exit-code-from register-connector register-connector
docker-compose up --exit-code-from wait-schema-registry-topic wait-schema-registry-topic
docker-compose up --exit-code-from mysql-client-seed mysql-client-seed
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from hive-tester hive-tester

docker-compose down

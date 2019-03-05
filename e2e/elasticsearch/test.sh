#!/bin/bash
set -e

docker-compose up -d elasticsearch
docker-compose up --exit-code-from elasticsearch-init elasticsearch-init
docker-compose up --exit-code-from spark-submit spark-submit
docker-compose up --exit-code-from elasticsearch-tester elasticsearch-tester
docker-compose down

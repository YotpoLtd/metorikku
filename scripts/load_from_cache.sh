#!/bin/bash
set -e

cp -r $TARGET_CACHE/target .

docker load -i $DOCKER_CACHE/spark.gz
docker load -i $DOCKER_CACHE/metorikku.gz
docker load -i $DOCKER_CACHE/hive.gz
docker load -i $DOCKER_CACHE/hive1.gz

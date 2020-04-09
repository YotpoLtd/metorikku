#!/bin/bash
set -e

mkdir -p $HOME/docker-cache
docker save metorikku/spark:latest | gzip -c > $DOCKER_CACHE/spark.gz
docker save metorikku/metorikku:latest | gzip -c > $DOCKER_CACHE/metorikku.gz
docker save metorikku/hive:latest | gzip -c > $DOCKER_CACHE/hive.gz
docker save metorikku/hive:1 | gzip -c > $DOCKER_CACHE/hive1.gz

mkdir -p $TARGET_CACHE
rm -rf target/streams/\$global/assembly target/streams/\$global/assemblyOption target/streams/test/assemblyOption target/streams/test/assembly
cp -r target $TARGET_CACHE

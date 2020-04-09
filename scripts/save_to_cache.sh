#!/bin/bash
set -e

mkdir -p $DOCKER_CACHE
echo "Saving spark docker to cache"
docker save metorikku/spark:latest | gzip -c > $DOCKER_CACHE/spark.gz
echo "Saving metorikku docker to cache"
docker save metorikku/metorikku:latest | gzip -c > $DOCKER_CACHE/metorikku.gz
echo "Saving hive docker to cache"
docker save metorikku/hive:latest | gzip -c > $DOCKER_CACHE/hive.gz
echo "Saving hive 1 docker to cache"
docker save metorikku/hive:1 | gzip -c > $DOCKER_CACHE/hive1.gz

mkdir -p $TARGET_CACHE
echo "Saving metorikku JAR to cache"
rm -rf target/streams/\$global/assembly target/streams/\$global/assemblyOption target/streams/test/assemblyOption target/streams/test/assembly
cp -r target $TARGET_CACHE

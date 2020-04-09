#!/bin/bash
set -e

# Hack that helps with the cache
docker pull $(grep -ioP '(?<=^from)\s+\S+' docker/spark/Dockerfile)
docker pull metorikku/spark:latest
docker pull metorikku/metorikku:latest
docker pull $(grep -ioP '(?<=^from)\s+\S+' docker/hive/Dockerfile)
docker pull metorikku/hive:latest
docker pull $(grep -ioP '(?<=^from)\s+\S+' docker/hive1/Dockerfile)
docker pull metorikku/hive:1

docker build -t metorikku/spark:$SPARK_VERSION --cache-from metorikku/spark:latest -t metorikku/spark:latest --build-arg SPARK_VERSION=$SPARK_VERSION --build-arg HADOOP_VERSION=$HADOOP_VERSION -f docker/spark/Dockerfile docker/spark
docker build -t metorikku/hive:1 --cache-from metorikku/hive:1 -t metorikku/hive:HIVE1_VERSION --build-arg HIVE_VERSION=$HIVE1_VERSION --build-arg HUDI_HIVE1_VERSION=$HUDI_HIVE1_VERSION -f docker/hive1/Dockerfile docker/hive1
docker build -t metorikku/hive --cache-from metorikku/hive -t metorikku/hive:HIVE_VERSION --build-arg HIVE_VERSION=$HIVE_VERSION --build-arg HUDI_VERSION=$HUDI_VERSION -f docker/hive/Dockerfile docker/hive
docker build -t metorikku/metorikku:latest -f docker/metorikku/Dockerfile .

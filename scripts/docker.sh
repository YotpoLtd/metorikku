#!/bin/bash

# Hack that helps with the cache
docker pull metorikku/metorikku:k8s
docker pull metorikku/metorikku:standalone
docker pull metorikku/metorikku:spark2_k8s
docker pull metorikku/metorikku:spark2_standalone
docker pull metorikku/hive

docker pull gcr.io/spark-operator/spark:v$SPARK_VERSION-gcs-prometheus
docker pull gcr.io/spark-operator/spark:v$SPARK2_VERSION-gcs-prometheus

set -e

# Latest spark
docker build -t metorikku/spark:k8s --cache-from metorikku/metorikku:k8s --build-arg SCALA_MAJOR_VERSION=$SCALA_MAJOR_VERSION --build-arg SPARK_VERSION=$SPARK_VERSION -f docker/spark/k8s/Dockerfile docker/spark/k8s
docker build -t metorikku/spark:standalone --cache-from metorikku/metorikku:standalone --build-arg IMAGE_NAME=metorikku/spark:k8s -f docker/spark/standalone/Dockerfile docker/spark/standalone
# Adding metorikku jar
docker build -t metorikku/metorikku:k8s --cache-from metorikku/metorikku:k8s --build-arg IMAGE_NAME=metorikku/spark:k8s -f docker/metorikku/Dockerfile .
docker build -t metorikku/metorikku:standalone --cache-from metorikku/metorikku:standalone --build-arg IMAGE_NAME=metorikku/spark:standalone -f docker/metorikku/Dockerfile .

# Spark 2
docker build -t metorikku/spark:spark2_k8s --cache-from metorikku/metorikku:spark2_k8s --build-arg SCALA_MAJOR_VERSION=$SPARK2_SCALA_MAJOR_VERSION --build-arg SPARK_VERSION=$SPARK2_VERSION --build-arg HADOOP_VERSION=${SPARK2_HADOOP_VERSION} -f docker/spark/k8s/Dockerfile docker/spark/k8s
docker build -t metorikku/spark:spark2_hadoop --cache-from metorikku/metorikku:spark2_standalone --build-arg IMAGE_NAME=metorikku/spark:spark2_k8s --build-arg HIVE_VERSION=${SPARK2_HIVE_VERSION} --build-arg HADOOP_VERSION=${SPARK2_HADOOP_VERSION} -f docker/spark/custom-hadoop/Dockerfile docker/spark/custom-hadoop
docker build -t metorikku/spark:spark2_standalone --cache-from metorikku/metorikku:spark2_standalone --build-arg IMAGE_NAME=metorikku/spark:spark2_hadoop -f docker/spark/standalone/Dockerfile docker/spark/standalone
# Adding metorikku jar
docker build -t metorikku/metorikku:spark2_standalone --cache-from metorikku/metorikku:spark2_standalone --build-arg IMAGE_NAME=metorikku/spark:spark2_standalone --build-arg SCALA_MAJOR_VERSION=${SPARK2_SCALA_MAJOR_VERSION} -f docker/metorikku/Dockerfile .
docker build -t metorikku/metorikku:spark2_k8s --cache-from metorikku/metorikku:spark2_k8s --build-arg IMAGE_NAME=metorikku/spark:spark2_hadoop --build-arg SCALA_MAJOR_VERSION=${SPARK2_SCALA_MAJOR_VERSION} -f docker/metorikku/Dockerfile .

# Hive
docker build -t metorikku/hive --cache-from metorikku/hive --build-arg HIVE_VERSION=$HIVE_VERSION --build-arg HUDI_VERSION=$HUDI_VERSION -f docker/hive/Dockerfile docker/hive

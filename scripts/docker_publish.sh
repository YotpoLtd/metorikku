#!/bin/bash
set -e

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker tag metorikku/metorikku metorikku/metorikku:"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag metorikku/spark metorikku/spark:$SPARK_VERSION
docker tag metorikku/k8s-spark-operator metorikku/k8s-spark-operator:$ SPARK_VERSION
docker tag metorikku/hive metorikku/hive:$HIVE_VERSION
docker tag metorikku/hive:1 metorikku/hive:$HIVE1_VERSION

docker push metorikku/spark
docker push metorikku/k8s-spark-operator
docker push metorikku/metorikku
docker push metorikku/hive
docker push metorikku/hive:1

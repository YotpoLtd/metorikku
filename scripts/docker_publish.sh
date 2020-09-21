#!/bin/bash
set -e

echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
docker tag metorikku/metorikku:standalone metorikku/metorikku:standalone_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag metorikku/metorikku:spark2_standalone metorikku/metorikku:standalone_"$TRAVIS_TAG"_spark_"$SPARK2_VERSION"
docker tag metorikku/metorikku:k8s metorikku/metorikku:k8s_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag metorikku/metorikku:spark2_k8s metorikku/metorikku:k8s_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag metorikku/hive metorikku/hive:$HIVE_VERSION

docker push metorikku/metorikku:standalone
docker push metorikku/metorikku:spark2_standalone
docker push metorikku/metorikku:k8s
docker push metorikku/metorikku:spark2_k8s
docker push metorikku/hive

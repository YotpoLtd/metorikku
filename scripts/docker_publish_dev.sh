#!/bin/bash
set -e

tag=$(([ ! "${TRAVIS_PULL_REQUEST}" = "false" ] && echo "${TRAVIS_PULL_REQUEST}") || echo "${TRAVIS_BRANCH}")

if [[ -z $DOCKER_PASSWORD ]]; then
  echo "Skipping pushing dev docker since no docker password was defined"
else
  echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
  docker tag metorikku/metorikku metorikku/dev:metorikku_$tag
  docker tag metorikku/spark metorikku/dev:spark_$tag
  docker tag metorikku/k8s-spark-operator metorikku/dev:k8s-spark-operator_$tag
  docker tag metorikku/hive metorikku/dev:hive_$tag
  docker tag metorikku/hive:1 metorikku/dev:hive1_$tag

  docker push metorikku/dev:metorikku_$tag
  docker push metorikku/dev:spark_$tag
  docker push metorikku/dev:k8s-spark-operator_$tag
  docker push metorikku/dev:hive_$tag
  docker push metorikku/dev:hive1_$tag
fi


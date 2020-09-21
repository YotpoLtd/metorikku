#!/bin/bash
set -e

tag=$(([ ! "${TRAVIS_PULL_REQUEST}" = "false" ] && echo "${TRAVIS_PULL_REQUEST}") || echo "${TRAVIS_BRANCH}")

if [[ -z $DOCKER_PASSWORD ]]; then
  echo "Skipping pushing dev docker since no docker password was defined"
else
  echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
  docker tag metorikku/metorikku:spark2_k8s metorikku/dev:metorikku_spark2_k8s_$tag
  docker tag metorikku/metorikku:spark2_standalone metorikku/dev:metorikku_spark2_standalone_$tag
  docker tag metorikku/metorikku:k8s metorikku/dev:metorikku_k8s_$tag
  docker tag metorikku/metorikku:standalone metorikku/dev:metorikku_standalone_$tag
  docker tag metorikku/hive metorikku/dev:hive_$tag

  docker push metorikku/dev:metorikku_spark2_k8s_$tag
  docker push metorikku/dev:metorikku_spark2_standalone_$tag
  docker push metorikku/dev:metorikku_k8s_$tag
  docker push metorikku/dev:metorikku_standalone_$tag
  docker push metorikku/dev:hive_$tag
fi


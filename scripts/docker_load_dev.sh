#!/bin/bash
set -e

tag=$(([ ! "${TRAVIS_PULL_REQUEST}" = "false" ] && echo "${TRAVIS_PULL_REQUEST}") || echo "${TRAVIS_BRANCH}")

docker pull metorikku/dev:metorikku_spark2_k8s_$tag
docker pull metorikku/dev:metorikku_spark2_standalone_$tag
docker pull metorikku/dev:metorikku_k8s_$tag
docker pull metorikku/dev:metorikku_standalone_$tag
docker pull metorikku/dev:hive_$tag

docker tag metorikku/dev:metorikku_standalone_$tag metorikku/metorikku:standalone
docker tag metorikku/dev:metorikku_k8s_$tag metorikku/metorikku:k8s
docker tag metorikku/dev:metorikku_spark2_k8s_$tag metorikku/metorikku:spark2_k8s
docker tag metorikku/dev:metorikku_spark2_standalone_$tag metorikku/metorikku:spark2_standalone
docker tag metorikku/dev:hive_$tag metorikku/hive

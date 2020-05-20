#!/bin/bash
set -e

tag=$(([ ! "${TRAVIS_PULL_REQUEST}" = "false" ] && echo "${TRAVIS_PULL_REQUEST}") || echo "${TRAVIS_BRANCH}")

docker pull metorikku/dev:metorikku_$tag
docker pull metorikku/dev:spark_$tag
docker pull metorikku/dev:hive_$tag
docker pull metorikku/dev:hive1_$tag

docker tag metorikku/dev:metorikku_$tag metorikku/metorikku
docker tag metorikku/dev:spark_$tag metorikku/spark
docker tag metorikku/dev:hive_$tag metorikku/hive
docker tag metorikku/dev:hive1_$tag metorikku/hive:1

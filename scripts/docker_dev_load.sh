#!/bin/bash
set -e

docker pull metorikku/dev:spark-"$TRAVIS_COMMIT"
docker pull metorikku/dev:metorikku-"$TRAVIS_COMMIT"
docker pull metorikku/dev:hive-"$TRAVIS_COMMIT"
docker pull metorikku/dev:hive1-"$TRAVIS_COMMIT"

docker tag metorikku/dev:spark-"$TRAVIS_COMMIT" metorikku/spark
docker tag metorikku/dev:metorikku-"$TRAVIS_COMMIT" metorikku/metorikku
docker tag metorikku/dev:hive-"$TRAVIS_COMMIT" metorikku/hive
docker tag metorikku/dev:hive1-"$TRAVIS_COMMIT" metorikku/hive1

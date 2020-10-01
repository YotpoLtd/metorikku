#!/bin/bash
set -e

pip install --user awscli
$(aws ecr get-login --region us-east-1 --no-include-email)

docker tag metorikku/metorikku:standalone $ECR_PATH/metorikku:standalone
docker tag metorikku/metorikku:spark2_standalone $ECR_PATH/metorikku:spark2_standalone
docker tag metorikku/metorikku:k8s $ECR_PATH/metorikku:k8s
docker tag metorikku/metorikku:spark2_k8s $ECR_PATH/metorikku:spark2_k8s

docker tag $ECR_PATH/metorikku:standalone $ECR_PATH/metorikku:standalone_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag $ECR_PATH/metorikku:spark2_standalone $ECR_PATH/metorikku:standalone_"$TRAVIS_TAG"_spark_"$SPARK2_VERSION"
docker tag $ECR_PATH/metorikku:k8s $ECR_PATH/metorikku:k8s_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag $ECR_PATH/metorikku:spark2_k8s $ECR_PATH/metorikku:k8s_"$TRAVIS_TAG"_spark_"$SPARK_VERSION"
docker tag metorikku/hive $ECR_PATH/hive:hive_$HIVE_VERSION

docker push $ECR_PATH/metorikku:standalone
docker push $ECR_PATH/metorikku:spark2_standalone
docker push $ECR_PATH/metorikku:k8s
docker push $ECR_PATH/metorikku:spark2_k8s
docker push $ECR_PATH/hive:hive_$HIVE_VERSION

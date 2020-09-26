#!/bin/bash
set -e

sbt +test
(cd examples/udf && sbt +package)

for SCALA in "${SPARK2_SCALA_MAJOR_VERSION}" "${SCALA_MAJOR_VERSION}" ; do
  echo "Testing metorikku with scala ${SCALA}"
  # Metorikku main test example
  java -Dspark.master=local[*] -cp target/scala-${SCALA}/metorikku-standalone_${SCALA}.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/movies_test.yaml
  # Metorikku kafka test example
  java -Dspark.master=local[*] -cp target/scala-${SCALA}/metorikku-standalone_${SCALA}.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/kafka/kafka2kafka_aggregations_test.yaml
  # Test UDF example
  java -Dspark.master=local[*] -cp target/scala-${SCALA}/metorikku-standalone_${SCALA}.jar:examples/udf/target/scala-${SCALA}/udf-example_${SCALA}-1.0.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/udf/udf_test.yaml
done

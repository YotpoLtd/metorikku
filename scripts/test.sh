#!/bin/bash
set -e

sbt test

# Metorikku main test example
java -Dspark.master=local[*] -cp target/scala-2.11/metorikku-standalone.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/movies_test.yaml
# Metorikku kafka test example
java -Dspark.master=local[*] -cp target/scala-2.11/metorikku-standalone.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/kafka/kafka2kafka_aggregations_test.yaml
# Test UDF example
(cd examples/udf && sbt package)
java -Dspark.master=local[*] -cp target/scala-2.11/metorikku-standalone.jar:examples/udf/target/scala-2.11/udf-example_2.11-1.0.jar com.yotpo.metorikku.MetorikkuTester --test-settings examples/udf/udf_test.yaml

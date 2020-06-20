ARG IMAGE_NAME=metorikku/spark:k8s
FROM ${IMAGE_NAME}

ARG SCALA_VERSION=2.12
ARG METORIKKU_JAR=target/scala-${SCALA_VERSION}/metorikku.jar

ADD $METORIKKU_JAR /
ADD examples ${SPARK_HOME}/work-dir/examples/
ADD examples /examples/

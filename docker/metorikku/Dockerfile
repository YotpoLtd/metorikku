ARG IMAGE_NAME=metorikku/spark:k8s
FROM ${IMAGE_NAME}

ARG SCALA_MAJOR_VERSION=2.12
ARG METORIKKU_JAR=target/scala-${SCALA_MAJOR_VERSION}/metorikku_${SCALA_MAJOR_VERSION}.jar

ADD $METORIKKU_JAR ./metorikku.jar
ADD examples ./examples/
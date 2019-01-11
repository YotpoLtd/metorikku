ARG SPARK_VERSION=latest

FROM metorikku/spark:$SPARK_VERSION

ARG METORIKKU_JAR=target/scala-2.11/metorikku.jar

ADD $METORIKKU_JAR /
ADD examples /examples/
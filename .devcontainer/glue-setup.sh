#!/usr/bin/env bash

INIT_FILE="$HOME/.aws-glue-libs.done"

ROOT_DIR="$(cd $(dirname "$0")/..; pwd)"
cd $ROOT_DIR

SPARK_CONF_DIR=$ROOT_DIR/conf
GLUE_JARS_DIR=$ROOT_DIR/jars

PYTHONPATH="$SPARK_HOME/python/:$PYTHONPATH"
PYTHONPATH="$(ls $SPARK_HOME/python/lib/py4j-*-src.zip):$PYTHONPATH"

GLUE_PY_FILES="$ROOT_DIR/PyGlue.zip"
export PYTHONPATH="$GLUE_PY_FILES:$PYTHONPATH"
export SPARK_CONF_DIR=${ROOT_DIR}/conf

if [[ ! -f "$INIT_FILE" ]]; then
    echo 'export PYTHONPATH='$PYTHONPATH >> "${HOME}/.bashrc"
    echo 'export SPARK_CONF_DIR='$SPARK_CONF_DIR >> "${HOME}/.bashrc"

    # Generate the zip archive for glue python modules
    zip -r PyGlue.zip awsglue

    # Run mvn copy-dependencies target to get the Glue dependencies locally
    mvn -f $ROOT_DIR/pom.xml -DoutputDirectory=$GLUE_JARS_DIR dependency:copy-dependencies

    # Generate spark-defaults.conf
    mkdir -p $SPARK_CONF_DIR
    echo "spark.driver.extraClassPath $(echo $SPARK_HOME/jars/* | tr ' ' ':'):$(echo $GLUE_JARS_DIR/* | tr ' ' ':')" >> $SPARK_CONF_DIR/spark-defaults.conf
    echo "spark.executor.extraClassPath $(echo $SPARK_HOME/jars/* | tr ' ' ':'):$(echo $GLUE_JARS_DIR/* | tr ' ' ':')" >> $SPARK_CONF_DIR/spark-defaults.conf

    touch "$INIT_FILE"
fi

# Restore present working directory
cd - > /dev/null

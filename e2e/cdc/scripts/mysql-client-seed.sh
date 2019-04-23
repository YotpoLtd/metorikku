#!/bin/bash

/scripts/wait_for_mysql.sh
MYSQL_HOST=${MYSQL_HOST:=mysql}
MYSQL_PORT=${MYSQL_PORT:=3306}
mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" ${MYSQL_DB} -e "source ${SCRIPT_PATH};"

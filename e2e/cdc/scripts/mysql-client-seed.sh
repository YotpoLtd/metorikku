#!/bin/bash

/scripts/wait_for_mysql.sh

mysql -h ${MYSQL_HOST} -P ${MYSQL_PORT} -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" ${MYSQL_DB} -e "source ${SCRIPT_PATH};"

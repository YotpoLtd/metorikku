#!/usr/bin/env bash
set -ev

HIVE_SERVER_PORT=${HIVE_SERVER_PORT:=10000}

echo Health check for Hive:
/opt/hive/bin/beeline -u "jdbc:hive2://127.0.0.1:${HIVE_SERVER_PORT}/default;auth=noSasl" -n health_check -e "show tables;"

#!/bin/bash
DYNAMODB=${DYNAMODB:=dynamodb:8000}
TABLE_NAME=${TABLE_NAME:=movies}

# wait for dynamodb to be ready
nc -z $DYNAMODB > /dev/null
until [ $? -eq "0" ];
do
  echo "Waiting for DynamoDB to be ready..."
  sleep 2s
done
echo "DynamoDB is ready!"

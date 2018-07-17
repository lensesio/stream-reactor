#!/bin/bash

CONNECT_PORT=8083
declare -a connectors=("hive", "redis")

for connector in "${connectors[@]}"
do
    echo "Running integration tests for $connector"

    CONNECTOR_PATH="kafka-connect-$connector"
    IT_PATH="$CONNECTOR_PATH/it"
    COMPOSE_FILE="$IT_PATH/docker-compose.yml"

    # if docker is present for this component, start it up, and wait for kafka connect to initialize
    if [ -e $COMPOSE_FILE ]
    then
        docker-compose -d -f $COMPOSE_FILE up

        for ((i=0;i<60;i++)); do
         sleep 5
         curl "http://localhost:$CONNECT_PORT/connector-plugins" | grep "FileStream" && break
        done

    fi

    ./gradlew -Pintegration-tests $IT_PATH:check

    # if docker is present for this component, shut it down
    if [ -e "$connector/docker-compose.yml" ]
    then
        docker-compose -f $COMPOSE_FILE stop
    fi

    echo "Completed integration tests for $connector"
done
#!/bin/bash

CONNECT_PORT=8083
declare -a connectors=("hive", "redis")

for connector in "${connectors[@]}"
do
   echo "Running integration tests for $connector"

    # if docker is present for this component, start it up, and wait for kafka connect to initialize
    if [ -e "$connector/docker-compose.yml" ]
    then
        docker-compose -d -f "$connector/docker-compose.yml" up

        for ((i=0;i<60;i++)); do
         sleep 5
         curl "http://localhost:$CONNECT_PORT/connector-plugins" | grep "FileStream" && break
        done

    fi

    ./gradlew :$connector:itest

    # if docker is present for this component, shut it down
    if [ -e "$connector/docker-compose.yml" ]
    then
        docker-compose -f "$connector/docker-compose.yml" stop
    fi

    echo "Completed integration tests for $connector"
done
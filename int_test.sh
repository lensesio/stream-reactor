#!/bin/bash

CONNECT_PORT=8083
declare -a connectors=("hive" "redis")
connectorslen=${#connectors[@]}

for (( i=0; i<${connectorslen}; i++ )); do
    CONNECTOR=${connectors[$i]}
    echo "Running integration tests for $CONNECTOR"

    CONNECTOR_PROJECT="kafka-connect-$CONNECTOR"
    echo "Connector path = $CONNECTOR_PROJECT"

    IT_PATH="$CONNECTOR_PROJECT/it"
    echo "IT test path = $IT_PATH"

    COMPOSE_FILE="$IT_PATH/docker-compose.yml"
    echo "Compose file = $COMPOSE_FILE"

    # if docker is present for this component, start it up, and wait for kafka connect to initialize
    if [ -e "$COMPOSE_FILE" ]
    then
        echo "Docker compose has been detected for $CONNECTOR; starting up as daemon"
        docker-compose -f $COMPOSE_FILE up -d

        for (( i=0 ; i<60 ; i++ )); do
            sleep 5
            curl "http://localhost:$CONNECT_PORT/connector-plugins" | grep "FileStream" && break
        done
    fi

    ./gradlew -Pintegration-tests $CONNECTOR_PROJECT:it:check

    # if docker is present for this component, shut it down
    if [ -e "$COMPOSE_FILE" ]
    then
        docker-compose -f $COMPOSE_FILE stop
    fi

    echo "Completed integration tests for $CONNECTOR"
done
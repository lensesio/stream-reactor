#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -jar ${DIR}/../libs/kafka-connect-cli-0.5-all.jar $@
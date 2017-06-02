#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

java -jar ${DIR}/kafka-connect-cli-@@CLI_VERSION@@-all.jar $@
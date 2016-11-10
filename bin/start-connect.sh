#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z ${CONFLUENT_HOME+x} ];
    then echo "CONFLUENT_HOME is unset";
    else echo "CONFLUENT_HOME is set to '$CONFLUENT_HOME'";
fi


for jar in ${DIR}/../libs/*; do
  CLASSPATH=${CLASSPATH}:${jar}
done

export CLASSPATH

${CONFLUENT_HOME}/bin/connect-distributed ${CONFLUENT_HOME}/etc/schema-registry/connect-avro-distributed.properties



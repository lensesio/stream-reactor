#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Download webserver, kafka topics ui and schema registry ui

wget https://github.com/mholt/caddy/releases/download/v0.9.3/caddy_linux_amd64.tar.gz
wget https://github.com/Landoop/kafka-topics-ui/releases/download/v0.7.3/kafka-topics-ui-0.7.3.tar.gz
wget https://github.com/Landoop/schema-registry-ui/releases/download/v.0.7.1/schema-registry-ui-0.7.1.tar.gz

# Create a directory and unpack them inside it
# .
# └── landoop-ui-tools
#     ├── caddy
#     └── public
#         ├── kafka-topics-ui
#         └── schema-registry-ui

mkdir -p ${DIR}/../landoop-ui-tools/caddy ${DIR}/../landoop-ui-tools/public/kafka-topics-ui ${DIR}/../landoop-ui-tools/public/schema-registry-ui

tar xf caddy_linux_amd64.tar.gz -C ${DIR}/../landoop-ui-tools/caddy
tar xf kafka-topics-ui-0.7.3.tar.gz -C ${DIR}/../landoop-ui-tools/public/kafka-topics-ui
tar xf schema-registry-ui-0.7.1.tar.gz -C ${DIR}/../landoop-ui-tools/public/schema-registry-ui

rm caddy_linux_amd64.tar.gz kafka-topics-ui-0.7.3.tar.gz schema-registry-ui-0.7.1.tar.gz

## Just an example if you want proper CORS straight from Schema Registry and Kafka REST
#echo "access.control.allow.methods=GET,POST,PUT,DELETE,OPTIONS" >> /opt/confluent-3.0.1/etc/schema-registry/schema-registry.properties
#echo 'access.control.allow.origin=*' >> /opt/confluent-3.0.1/etc/schema-registry/schema-registry.properties
#echo "access.control.allow.methods=GET,POST,PUT,DELETE,OPTIONS" >> /opt/confluent-3.0.1/etc/kafka-rest/kafka-rest.properties
#echo 'access.control.allow.origin=*' >> /opt/confluent-3.0.1/etc/kafka-rest/kafka-rest.properties

# Make Kafka Topics UI and Schema Registry UI to use the proxied kafka rest and schema registry
# to avoid CORS
sed -i -e 's|http://localhost:8081|/api/schema-registry|' ${DIR}/../landoop-ui-tools/public/schema-registry-ui/combined.js
sed -i -e 's|http://localhost:8082|/api/kafka-rest-proxy|' ${DIR}/../landoop-ui-tools/public/kafka-topics-ui/combined.js

# Create caddy (server) configuration:
cat <<EOF > ${DIR}/../landoop-ui-tools/Caddyfile
0.0.0.0:3030
tls off

root "${DIR}/../landoop-ui-tools/public"
browse /

proxy /api/schema-registry localhost:8081 {
    without /api/schema-registry
}

proxy /api/kafka-rest-proxy localhost:8082 {
    without /api/kafka-rest-proxy
}

proxy /api/kafka-connect localhost:8083 {
    without /api/kafka-connect
}
EOF

# How to run it:
echo -e "\e[44mTo run the UI:\e[0m"
echo -e " \e[32mstart-ui.sh\e[0m"
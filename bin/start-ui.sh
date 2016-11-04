#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

${DIR}/../landoop-ui/caddy/caddy_linux_amd64 -conf ${DIR}/../landoop-ui-tools/Caddyfile
#!/usr/bin/env bash
# Tear down the local ADLS dev environment.
#
# Removes:
#   - Docker Compose stack (containers + volumes)
#   - Generated connector jar (connectors/*.jar)
#
# Does NOT delete the remote ADLS filesystem or storage account — those are
# cloud resources that require explicit cleanup. See the note printed below.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="${SCRIPT_DIR}/.."
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
CONNECTORS_DIR="${COMPOSE_DIR}/connectors"

echo "==> [99-teardown] stopping Docker Compose stack ..."
docker compose -f "${COMPOSE_FILE}" down -v --remove-orphans 2>/dev/null || true

echo "==> [99-teardown] removing generated connector jars ..."
find "${CONNECTORS_DIR}" -name "*.jar" -delete 2>/dev/null || true

echo "==> [99-teardown] done."
echo ""
echo "  NOTE: The remote ADLS filesystem and storage account have NOT been deleted."
echo "  To clean up the Azure resources, run:"
echo "    az group delete -n <your-resource-group> --yes --no-wait"
echo "  or delete the storage account directly in the Azure Portal."

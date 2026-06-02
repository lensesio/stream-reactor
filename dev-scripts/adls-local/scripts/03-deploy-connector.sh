#!/usr/bin/env bash
# Deploy the adls-dev connector to the local Kafka Connect worker.
#
# Required env vars:
#   AZURE_STORAGE_CONNECTION_STRING  — full connection string from the Azure portal
#                                      (DefaultEndpointsProtocol=https;AccountName=...;...)
#
# Optional env vars:
#   ADLS_FILESYSTEM   — ADLS Gen2 filesystem (container) name  (default: adls-dev)
#
# What it does:
#   1. Validates env vars.
#   2. Renders the final connector JSON by substituting placeholders.
#   3. PUTs the config to the Kafka Connect REST API.
#   4. Polls the /status endpoint until connector + all 3 tasks are RUNNING.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_TEMPLATE="${SCRIPT_DIR}/../connector-config.json"
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="adls-dev"
ADLS_FILESYSTEM="${ADLS_FILESYSTEM:-adls-dev}"

# ── Validate required env vars ────────────────────────────────────────────────
if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" ]]; then
  echo "ERROR: AZURE_STORAGE_CONNECTION_STRING is not set."
  echo ""
  echo "  Export the connection string for your ADLS Gen2 storage account:"
  echo "    export AZURE_STORAGE_CONNECTION_STRING=\"DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net\""
  echo ""
  echo "  See the README for instructions on obtaining this value from the Azure portal or az CLI."
  exit 1
fi

# ── Validate config template ──────────────────────────────────────────────────
if [[ ! -f "${CONFIG_TEMPLATE}" ]]; then
  echo "ERROR: connector config template not found at ${CONFIG_TEMPLATE}"
  exit 1
fi

# ── Render final JSON (substitute placeholders) ───────────────────────────────
TMP_CONFIG=$(mktemp /tmp/adls-connector-config.XXXXXX.json)
trap 'rm -f "${TMP_CONFIG}"' EXIT

# Escape the connection string for sed (it may contain / and & and special chars).
# We use | as sed delimiter to avoid issues with / in the value.
ESCAPED_CONN=$(printf '%s\n' "${AZURE_STORAGE_CONNECTION_STRING}" | sed 's/[\\&|]/\\&/g')
ESCAPED_FS=$(printf '%s\n' "${ADLS_FILESYSTEM}" | sed 's/[\\&|]/\\&/g')

sed \
  -e "s|REPLACE_ME_CONNECTION_STRING|${ESCAPED_CONN}|g" \
  -e "s|REPLACE_ME_FILESYSTEM|${ESCAPED_FS}|g" \
  "${CONFIG_TEMPLATE}" > "${TMP_CONFIG}"

echo "==> [03-deploy-connector] rendered config (connection string redacted):"
if command -v jq &>/dev/null; then
  jq 'del(."connect.datalake.azure.connection.string")' "${TMP_CONFIG}"
else
  grep -v "connection.string" "${TMP_CONFIG}" || cat "${TMP_CONFIG}"
fi
echo ""

# ── Wait for Connect REST API ─────────────────────────────────────────────────
echo "==> [03-deploy-connector] waiting for Kafka Connect REST API at ${CONNECT_URL} ..."
for i in {1..30}; do
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/" 2>/dev/null || true)
  if [[ "${HTTP}" == "200" ]]; then
    echo "    Connect is up."
    break
  fi
  echo "    attempt ${i}/30 — HTTP ${HTTP}, sleeping 5 s ..."
  sleep 5
done

HTTP=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/" 2>/dev/null || true)
if [[ "${HTTP}" != "200" ]]; then
  echo "ERROR: Connect REST API did not become available."
  exit 1
fi

# ── Create or update the connector ───────────────────────────────────────────
echo "==> [03-deploy-connector] deploying connector '${CONNECTOR_NAME}' ..."
HTTP=$(curl -s -o /tmp/connect-response.json -w "%{http_code}" \
  -X PUT \
  -H "Content-Type: application/json" \
  -d "@${TMP_CONFIG}" \
  "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" 2>/dev/null)

echo "    response HTTP ${HTTP}:"
(command -v jq &>/dev/null && jq . /tmp/connect-response.json || cat /tmp/connect-response.json)
echo ""

if [[ "${HTTP}" != "200" && "${HTTP}" != "201" ]]; then
  echo "ERROR: connector deployment failed (HTTP ${HTTP})."
  exit 1
fi

# ── Poll for RUNNING (both connector AND all tasks) ───────────────────────────
echo "==> [03-deploy-connector] waiting for connector and tasks to reach RUNNING ..."
for i in {1..30}; do
  STATUS_JSON=$(curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null || true)

  if command -v jq &>/dev/null; then
    CONN_STATE=$(echo "${STATUS_JSON}" | jq -r '.connector.state // "unknown"')
    TASK_COUNT=$(echo "${STATUS_JSON}" | jq -r '.tasks // [] | length')
    FAILED_TASKS=$(echo "${STATUS_JSON}" | jq -r '[.tasks // [] | .[] | select(.state == "FAILED")] | length')
    RUNNING_TASKS=$(echo "${STATUS_JSON}" | jq -r '[.tasks // [] | .[] | select(.state == "RUNNING")] | length')
  else
    CONN_STATE=$(echo "${STATUS_JSON}" | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
    TASK_COUNT=0
    FAILED_TASKS=0
    RUNNING_TASKS=0
  fi

  echo "    attempt ${i}/30 — connector=${CONN_STATE:-unknown} tasks=${RUNNING_TASKS}/${TASK_COUNT} running, ${FAILED_TASKS} failed"

  # Any task failure → bail out immediately with full diagnostics
  if [[ "${FAILED_TASKS}" -gt 0 ]]; then
    echo ""
    echo "ERROR: ${FAILED_TASKS} task(s) are in FAILED state."
    echo "──────────────────────────────────────────────────"
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && \
      jq '.tasks[] | select(.state == "FAILED") | {id: .id, state: .state, trace: .trace}' || cat)
    echo "──────────────────────────────────────────────────"
    echo ""
    echo "Connect worker logs (last 40 lines):"
    echo "──────────────────────────────────────────────────"
    docker logs connect-adls 2>&1 | tail -40
    echo "──────────────────────────────────────────────────"
    exit 1
  fi

  if [[ "${CONN_STATE}" == "FAILED" ]]; then
    echo "ERROR: connector itself entered FAILED state."
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && jq . || cat)
    exit 1
  fi

  # All-good condition: connector RUNNING and all tasks RUNNING
  if [[ "${CONN_STATE}" == "RUNNING" && "${TASK_COUNT}" -gt 0 && "${RUNNING_TASKS}" -eq "${TASK_COUNT}" ]]; then
    echo "==> [03-deploy-connector] connector and all ${TASK_COUNT} task(s) are RUNNING."
    echo ""
    echo "${STATUS_JSON}" | (command -v jq &>/dev/null && jq . || cat)
    exit 0
  fi

  sleep 5
done

echo "ERROR: connector did not reach a healthy state within the timeout."
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | (command -v jq &>/dev/null && jq . || cat)
echo ""
echo "Connect worker logs (last 40 lines):"
docker logs connect-adls 2>&1 | tail -40
exit 1

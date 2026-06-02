# Azure Data Lake Storage (ADLS) local dev environment

Spins up a full local stack — Kafka 4.1 (KRaft) and Kafka Connect 4.1 — so you can exercise the `kafka-connect-azure-datalake` sink connector from this repo end-to-end against a real Azure Data Lake Storage Gen2 account, without any other infrastructure.

## What it does

1. Builds the connector fat-jar from source with JDK 17.
2. Starts Docker Compose (Kafka, Connect).
3. Creates topic `adls-dev-topic` with **3 partitions**.
4. Deploys the connector with `connect.datalake.azure.auth.mode=connectionstring`.
5. Produces 6 sample JSON records to the topic (2 per partition).
6. Verifies JSON files appear in the ADLS Gen2 filesystem (one file per record, due to `WITH_FLUSH_COUNT = 1`).

## Prerequisites

| Requirement | Notes |
|---|---|
| Docker + Docker Compose v2 | `docker compose version` must work |
| JDK 17 | Used to build the connector via sbt. macOS: `brew install --cask temurin@17` |
| sbt | [scala-sbt.org/download](https://www.scala-sbt.org/download/) |
| `curl`, `jq` | For deploy polling and verification |
| Azure account | Free tier sufficient; see below |
| `az` CLI (optional) | Used in `05-verify.sh` to count files; falls back to Portal instructions if absent |

## Getting Azure credentials

You need an **ADLS Gen2** storage account (hierarchical namespace enabled) and a filesystem (container) inside it. The connector authenticates with a **connection string**.

### Option A — Azure Portal

1. Go to the [Azure Portal](https://portal.azure.com) → **Create a resource** → search **Storage account** → Create.
2. Fill in:
   - **Resource group**: create a new one, e.g. `streamreactor-dev`.
   - **Storage account name**: a globally-unique lowercase name, 3–24 characters, e.g. `srdev12345`.
   - **Region**: any region near you.
   - **Redundancy**: Locally-redundant storage (LRS) is cheapest.
3. Click the **Advanced** tab → tick **"Enable hierarchical namespace"** — this is what makes it ADLS Gen2 (not plain blob storage). The connector requires this.
4. Leave all other settings at defaults. Click **Review + create** → **Create**.
5. Once deployed, open the storage account → **Data storage** → **Containers** → **+ Container** → name it `adls-dev` (this must match `ADLS_FILESYSTEM`). Leave public access as **Private**.
6. In the same storage account → **Security + networking** → **Access keys** → click **Show keys** → copy the **Connection string** for `key1`.

Export it:

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=srdev12345;AccountKey=...;EndpointSuffix=core.windows.net"
export ADLS_FILESYSTEM="adls-dev"
```

### Option B — Azure CLI

Requires `az login` to be done first (`brew install azure-cli` or [install docs](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)).

```bash
RG=streamreactor-dev
LOC=westeurope
ACC=srdev$RANDOM      # globally unique, lowercase, 3-24 chars
FS=adls-dev

# Create resource group
az group create -n "$RG" -l "$LOC"

# Create ADLS Gen2 storage account (--hierarchical-namespace true is the key flag)
az storage account create \
  -n "$ACC" -g "$RG" -l "$LOC" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# Retrieve connection string
CONN=$(az storage account show-connection-string -n "$ACC" -g "$RG" -o tsv)

# Create the filesystem (container)
az storage fs create -n "$FS" --connection-string "$CONN"

# Export for the setup script
export AZURE_STORAGE_CONNECTION_STRING="$CONN"
export ADLS_FILESYSTEM="$FS"
```

## Quick start

```bash
# Run everything in one go (from repo root):
./dev-scripts/adls-local/scripts/00-setup.sh

# Or step by step:
./dev-scripts/adls-local/scripts/01-build-connector.sh
docker compose -f dev-scripts/adls-local/docker-compose.yml up -d
./dev-scripts/adls-local/scripts/02-create-topics.sh
./dev-scripts/adls-local/scripts/03-deploy-connector.sh
./dev-scripts/adls-local/scripts/04-produce-data.sh
./dev-scripts/adls-local/scripts/05-verify.sh
```

## Rebuilding the connector jar

```bash
REBUILD=1 ./dev-scripts/adls-local/scripts/01-build-connector.sh
```

## Teardown

```bash
./dev-scripts/adls-local/scripts/99-teardown.sh
```

Removes the Docker Compose stack (including volumes) and the generated jar.  
**The remote ADLS filesystem and storage account are not deleted** (safety). To clean up Azure resources:

```bash
# Delete the entire resource group (removes storage account + filesystem)
az group delete -n streamreactor-dev --yes --no-wait
```

## Config reference

### Filesystem naming rules

The `ADLS_FILESYSTEM` value is the ADLS Gen2 container name. Naming constraints (enforced by the connector):

- Lowercase letters, numbers and hyphens (`-`) only.
- Must start and end with a letter or number.
- No consecutive hyphens.
- 3–63 characters long.

### Connector config key reference

| Key | Value in this script | Notes |
|---|---|---|
| `connector.class` | `io.lenses.streamreactor.connect.datalake.sink.DatalakeSinkConnector` | ADLS sink |
| `tasks.max` | `3` | One task per partition |
| `topics` | `adls-dev-topic` | |
| `connect.datalake.kcql` | `INSERT INTO <fs>:adls-dev SELECT * FROM adls-dev-topic STOREAS \`JSON\` WITH_FLUSH_COUNT = 1` | Writes to `<filesystem>/adls-dev/` directory; 1 file per record |
| `connect.datalake.azure.auth.mode` | `connectionstring` | See auth modes below |
| `connect.datalake.azure.connection.string` | *(substituted at deploy time)* | Never stored on disk |
| `connect.datalake.local.tmp.directory` | `/tmp/adls-staging` | Local staging directory inside the Connect container |

The KCQL path format is `<filesystem>:<directory>`. The filesystem is the ADLS container name; the directory is the path within it where files are written.

### Switching authentication mode

Three auth modes are supported (set via `connect.datalake.azure.auth.mode`):

**`connectionstring`** (used by this script) — single string from the portal containing account name, key, and endpoint. Simplest for local dev; does not require separate account name/key fields. Requires "Allow storage account key access" to be enabled on the account.

**`credentials`** — account name + account key separately:

```json
{
  "connect.datalake.azure.auth.mode": "credentials",
  "connect.datalake.azure.account.name": "srdev12345",
  "connect.datalake.azure.account.key": "<your-account-key>",
  "connect.datalake.endpoint": "https://srdev12345.dfs.core.windows.net"
}
```

**`default`** — uses the Azure DefaultAzureCredential chain (environment variables → managed identity → `az login` token). Requires the credential to be available _inside the Kafka Connect container_, not just on the host. For local dev this means setting `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` as Docker environment variables, or mounting a token file:

```yaml
# docker-compose.yml addition
environment:
  AZURE_CLIENT_ID: "..."
  AZURE_CLIENT_SECRET: "..."
  AZURE_TENANT_ID: "..."
```

### How the connection string is stored

The connection string is PUT to Kafka Connect via its REST API at deploy time. It is stored inside the `connect-configs` Kafka topic (inside the local Docker Compose stack) — not on disk in this repository. The `connector-config.json` file committed to git contains only the `REPLACE_ME_CONNECTION_STRING` placeholder.

## Verification

After `00-setup.sh` completes:

- `curl http://localhost:8083/connectors/adls-dev/status | jq .` returns `state: "RUNNING"` with all 3 tasks running.
- The ADLS filesystem contains 6 JSON files (one per produced record).

```bash
# Check connector status
curl http://localhost:8083/connectors/adls-dev/status | jq .

# List files in ADLS (requires az CLI)
az storage fs file list \
  --file-system "${ADLS_FILESYSTEM}" \
  --connection-string "${AZURE_STORAGE_CONNECTION_STRING}" \
  --recursive \
  --output table
```

## Cost note

A Standard LRS StorageV2 account costs fractions of a cent at this data volume (a few kilobytes of JSON). The account accrues a small standing charge (~$0.02/GB/month for LRS in West Europe) while it exists. Delete the resource group when done to avoid any ongoing cost:

```bash
az group delete -n streamreactor-dev --yes
```

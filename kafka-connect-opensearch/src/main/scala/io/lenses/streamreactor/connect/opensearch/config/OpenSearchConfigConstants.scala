/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.opensearch.config

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants._

/**
 * OpenSearch-specific configuration constants.
 *
 * Connector prefix: `connect.opensearch`
 */
object OpenSearchConfigConstants {

  val CONNECTOR_PREFIX = "connect.opensearch"

  // Full keys (prefix + common suffixes)
  val PROTOCOL             = s"$CONNECTOR_PREFIX.$PROTOCOL_SUFFIX"
  val HOSTS                = s"$CONNECTOR_PREFIX.$HOSTS_SUFFIX"
  val ES_PORT              = s"$CONNECTOR_PREFIX.$ES_PORT_SUFFIX"
  val ES_PREFIX            = s"$CONNECTOR_PREFIX.$ES_PREFIX_SUFFIX"
  val KCQL                 = s"$CONNECTOR_PREFIX.$KCQL_SUFFIX"
  val WRITE_TIMEOUT        = s"$CONNECTOR_PREFIX.$WRITE_TIMEOUT_SUFFIX_"
  val NBR_OF_RETRIES       = s"$CONNECTOR_PREFIX.$NBR_OF_RETRIES_SUFFIX"
  val ERROR_POLICY         = s"$CONNECTOR_PREFIX.$ERROR_POLICY_SUFFIX"
  val BATCH_SIZE           = s"$CONNECTOR_PREFIX.$BATCH_SIZE_SUFFIX"
  val ERROR_RETRY_INTERVAL = s"$CONNECTOR_PREFIX.$ERROR_RETRY_INTERVAL_SUFFIX"
  val PK_JOINER_SEPARATOR  = s"$CONNECTOR_PREFIX.$PK_JOINER_SEPARATOR_SUFFIX"

  val CLIENT_HTTP_BASIC_AUTH_USERNAME = s"$CONNECTOR_PREFIX.$CLIENT_HTTP_BASIC_AUTH_USERNAME_SUFFIX"
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD = s"$CONNECTOR_PREFIX.$CLIENT_HTTP_BASIC_AUTH_PASSWORD_SUFFIX"

  // JWT authentication keys
  val JWT_TOKEN_KEY     = s"$CONNECTOR_PREFIX.security.jwt.token"
  val JWT_TOKEN_DOC     = """Static JWT bearer token. STATIC JWT does NOT auto-refresh. When the token expires,
                        |the cluster returns HTTP 401 and the task fails per error.policy. For production deployments,
                        |use connect.opensearch.security.jwt.token.file with a path written by your IdP rotation
                        |tooling and tune connect.opensearch.security.jwt.token.refresh.interval.ms to match the
                        |IdP's rotation cadence.""".stripMargin
  val JWT_TOKEN_DEFAULT = ""

  val JWT_TOKEN_FILE_KEY = s"$CONNECTOR_PREFIX.security.jwt.token.file"
  val JWT_TOKEN_FILE_DOC =
    "Path to a file containing the JWT bearer token. Re-read at the configured interval. Mutually exclusive with connect.opensearch.security.jwt.token."
  val JWT_TOKEN_FILE_DEFAULT = ""

  val JWT_REFRESH_INTERVAL_KEY     = s"$CONNECTOR_PREFIX.security.jwt.token.refresh.interval.ms"
  val JWT_REFRESH_INTERVAL_DOC     = "How often (in ms) to re-read the JWT token file. Must be > 0. Default is 60000ms."
  val JWT_REFRESH_INTERVAL_DEFAULT = 60000L

  val JWT_TOKEN_BASE_DIR_KEY = s"$CONNECTOR_PREFIX.security.jwt.token.base.dir"
  val JWT_TOKEN_BASE_DIR_DOC =
    """Restricts JWT token file reads to this directory (and its subdirectories).
      |When set, any path that resolves outside the base directory is rejected at read time.
      |Strongly recommended in shared or multi-tenant Connect deployments to prevent a connector
      |config from being used to read arbitrary worker-local files.""".stripMargin
  val JWT_TOKEN_BASE_DIR_DEFAULT = ""

  // AWS SigV4 authentication keys
  val AWS_SIGNING_ENABLED_KEY = s"$CONNECTOR_PREFIX.aws.signing.enabled"
  val AWS_SIGNING_ENABLED_DOC =
    "Enable AWS SigV4 request signing. When true, the transport switches from RestClientTransport to AwsSdk2Transport."
  val AWS_SIGNING_ENABLED_DEFAULT = false

  val AWS_REGION_KEY = s"$CONNECTOR_PREFIX.aws.region"
  val AWS_REGION_DOC =
    "AWS region. Required when aws.signing.enabled=true. Validated against the SDK's known-region list at config time."
  val AWS_REGION_DEFAULT = ""

  val AWS_SIGNING_SERVICE_KEY = s"$CONNECTOR_PREFIX.aws.signing.service"
  val AWS_SIGNING_SERVICE_DOC =
    "AWS service name for SigV4 signing. Must be one of {es, aoss}. 'es' for Managed OpenSearch, 'aoss' for OpenSearch Serverless."
  val AWS_SIGNING_SERVICE_DEFAULT = "es"

  val AWS_CREDENTIALS_PROVIDER_KEY = s"$CONNECTOR_PREFIX.aws.credentials.provider"
  val AWS_CREDENTIALS_PROVIDER_DOC =
    """Credentials provider type. DEFAULT or STATIC.
      |Use DEFAULT for production deployments running on EC2 / EKS / Lambda — the SDK chain handles
      |credential refresh transparently. STATIC is for local development, air-gapped environments,
      |or scripted tests against WireMock; STATIC keys live in the worker connector config and are
      |not rotated by the connector.""".stripMargin
  val AWS_CREDENTIALS_PROVIDER_DEFAULT = "DEFAULT"

  val AWS_ACCESS_KEY_ID_KEY     = s"$CONNECTOR_PREFIX.aws.access.key.id"
  val AWS_ACCESS_KEY_ID_DOC     = "AWS access key ID. Required when aws.credentials.provider=STATIC."
  val AWS_ACCESS_KEY_ID_DEFAULT = ""

  val AWS_SECRET_ACCESS_KEY_KEY     = s"$CONNECTOR_PREFIX.aws.secret.access.key"
  val AWS_SECRET_ACCESS_KEY_DOC     = "AWS secret access key. Required when aws.credentials.provider=STATIC."
  val AWS_SECRET_ACCESS_KEY_DEFAULT = ""

  val AWS_SESSION_TOKEN_KEY = s"$CONNECTOR_PREFIX.aws.session.token"
  val AWS_SESSION_TOKEN_DOC =
    "AWS session token. Optional, for STS / assumed-role workflows. AWS session token configured via connect.opensearch.aws.session.token; this token does NOT auto-refresh and the connector will fail when it expires. Use connect.opensearch.aws.credentials.provider=DEFAULT for production deployments."
  val AWS_SESSION_TOKEN_DEFAULT = ""

  // Strict bulk item errors
  val BULK_STRICT_ITEM_ERRORS_KEY = s"$CONNECTOR_PREFIX.bulk.strict.item.errors"
  val BULK_STRICT_ITEM_ERRORS_DOC =
    """When true (default), any per-item bulk failure goes through ErrorPolicy.
      |HTTP 429 / es_rejected_execution_exception is classified as retriable (RetriableIntegrityException);
      |mapper conflicts and other permanent item errors are FatalConnectException.
      |When false, only HTTP-transport errors are surfaced (legacy tolerant mode).
      |WARNING: Setting bulk.strict.item.errors=false swallows rejected documents and advances Kafka offsets past them.""".stripMargin
  val BULK_STRICT_ITEM_ERRORS_DEFAULT = true

  // Connection pool tuning (REST / HC5 path only; SigV4 path uses AWS SDK defaults)
  val MAX_CONNECTIONS_PER_ROUTE_KEY = s"$CONNECTOR_PREFIX.max.connections.per.route"
  val MAX_CONNECTIONS_PER_ROUTE_DOC =
    "Maximum number of HTTP connections to keep open per OpenSearch node (HC5 connection pool). " +
      "Applies to the REST transport path only; SigV4 uses the AWS SDK's own connection pool. " +
      "Default: 5 (HC5 PoolingAsyncClientConnectionManager default). " +
      "Increase for high-throughput deployments where many batches are in-flight concurrently."
  val MAX_CONNECTIONS_PER_ROUTE_DEFAULT = 5

  val MAX_CONNECTIONS_TOTAL_KEY = s"$CONNECTOR_PREFIX.max.connections.total"
  val MAX_CONNECTIONS_TOTAL_DOC =
    "Maximum total number of HTTP connections across all OpenSearch nodes (HC5 connection pool). " +
      "Applies to the REST transport path only. " +
      "Default: 25 (HC5 PoolingAsyncClientConnectionManager default). " +
      "Set to at least max.connections.per.route × number of nodes."
  val MAX_CONNECTIONS_TOTAL_DEFAULT = 25

  // Security: cleartext-auth guard opt-out
  val ALLOW_INSECURE_AUTH_KEY = s"$CONNECTOR_PREFIX.security.allow.insecure.auth"
  val ALLOW_INSECURE_AUTH_DOC =
    """By default the connector rejects protocol=http when HTTP Basic auth or JWT bearer authentication is
      |configured, because credentials would be transmitted over cleartext and could be intercepted by an
      |on-path attacker.  Set this to true ONLY when TLS termination is handled by a trusted local
      |sidecar or service mesh so that the connector itself communicates over unencrypted HTTP while
      |end-to-end encryption is still guaranteed externally.  A WARN-level message is emitted at startup
      |when this override is active.""".stripMargin
  val ALLOW_INSECURE_AUTH_DEFAULT = false
}

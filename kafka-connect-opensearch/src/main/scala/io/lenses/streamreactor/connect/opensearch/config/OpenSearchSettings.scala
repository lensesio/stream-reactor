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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import io.lenses.streamreactor.connect.opensearch.auth.JwtTokenSource
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import org.apache.kafka.common.config.ConfigException
import software.amazon.awssdk.regions.Region

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * OpenSearch-specific settings, extending [[ElasticCommonSettings]].
 */
case class OpenSearchSettings(
  common:                 ElasticCommonSettings,
  hosts:                  Seq[String],
  port:                   Int,
  tablePrefix:            Option[String],
  protocol:               String,
  awsSigningEnabled:      Boolean,
  awsRegion:              String,
  awsSigningService:      String,
  awsCredentialsProvider: String,
  awsAccessKeyId:         String,
  awsSecretAccessKey:     String,
  awsSessionToken:        String,
  jwtTokenSource:         Option[JwtTokenSource],
  strictItemErrors:       Boolean,
  maxConnectionsPerRoute: Int,
  maxConnectionsTotal:    Int,
) {

  /** The SigV4 host string: bare hostname when port == 9200, else `host:port`. */
  def sigV4Host: String = {
    val host = hosts.head
    if (port == 9200) host else s"$host:$port"
  }
}

object OpenSearchSettings extends StrictLogging {

  def apply(config: OpenSearchConfig): OpenSearchSettings = {
    // Parity row 53: split on "," without trim — mirrors ElasticWriter.endpoints in elastic7.
    // Whitespace inside the comma-separated list is preserved verbatim on the REST path.
    // The SigV4 path enforces a single-host + no-scheme constraint separately (lines below).
    val hostsRaw    = config.getString(HOSTS).split(",").toSeq
    val port        = config.getInt(ES_PORT)
    val protocol    = config.getString(PROTOCOL)
    val tablePrefix = Option(config.getString(ES_PREFIX)).filter(_.nonEmpty)

    val awsSigningEnabled  = config.getBoolean(AWS_SIGNING_ENABLED_KEY)
    val awsRegion          = config.getString(AWS_REGION_KEY)
    val awsSigningService  = config.getString(AWS_SIGNING_SERVICE_KEY)
    val awsCredProvider    = config.getString(AWS_CREDENTIALS_PROVIDER_KEY)
    val awsAccessKeyId     = config.getString(AWS_ACCESS_KEY_ID_KEY)
    val awsSecretAccessKey = config.getPassword(AWS_SECRET_ACCESS_KEY_KEY).value()
    val awsSessionToken    = config.getPassword(AWS_SESSION_TOKEN_KEY).value()

    val jwtToken     = config.getPassword(JWT_TOKEN_KEY).value()
    val jwtTokenFile = config.getString(JWT_TOKEN_FILE_KEY)
    val jwtRefreshMs = config.getLong(JWT_REFRESH_INTERVAL_KEY)
    val jwtBaseDir   = Option(config.getString(JWT_TOKEN_BASE_DIR_KEY)).filter(_.nonEmpty)

    val username = config.getString(CLIENT_HTTP_BASIC_AUTH_USERNAME)
    val password = config.getPassword(CLIENT_HTTP_BASIC_AUTH_PASSWORD).value()

    val strictItemErrors = config.getBoolean(BULK_STRICT_ITEM_ERRORS_KEY)

    // --- Validations ---

    // cluster.name is an Elasticsearch concept — reject it early so users of the ES7 connector
    // who copy their config file don't silently have this property ignored.
    val clusterNameKey = s"$CONNECTOR_PREFIX.cluster.name"
    Option(config.originalsStrings().get(clusterNameKey)).filter(_.nonEmpty).foreach { v =>
      throw new ConfigException(
        s"$clusterNameKey is not supported by the OpenSearch connector (got '$v'). " +
          "This is an Elasticsearch concept; remove it from your connector config.",
      )
    }

    // tableprefix is not supported on any OpenSearch transport (httpclient5 has no path-prefix API)
    if (tablePrefix.nonEmpty) {
      throw new ConfigException(
        "connect.opensearch.tableprefix is not supported by the OpenSearch connector; " +
          "path-prefix is not available in the ApacheHttpClient5TransportBuilder. " +
          "This is an intentional difference from elastic6/7.",
      )
    }

    // JWT static + JWT file mutually exclusive
    if (jwtToken.nonEmpty && jwtTokenFile.nonEmpty) {
      throw new ConfigException(
        "connect.opensearch.security.jwt.token and connect.opensearch.security.jwt.token.file are mutually exclusive; set one or neither",
      )
    }

    // JWT token refresh interval must be > 0 when file is set
    if (jwtTokenFile.nonEmpty && jwtRefreshMs <= 0) {
      throw new ConfigException(
        s"connect.opensearch.security.jwt.token.refresh.interval.ms must be > 0; got $jwtRefreshMs",
      )
    }

    // JWT and basic auth mutually exclusive
    if ((jwtToken.nonEmpty || jwtTokenFile.nonEmpty) && (username.nonEmpty || password.nonEmpty)) {
      throw new ConfigException(
        "JWT authentication and HTTP basic auth are mutually exclusive; configure one or neither",
      )
    }

    // SigV4 mutually exclusive with basic auth and JWT
    if (awsSigningEnabled && (username.nonEmpty || password.nonEmpty)) {
      throw new ConfigException(
        "AWS SigV4 signing and HTTP basic auth are mutually exclusive",
      )
    }
    if (awsSigningEnabled && (jwtToken.nonEmpty || jwtTokenFile.nonEmpty)) {
      throw new ConfigException(
        "AWS SigV4 signing and JWT authentication are mutually exclusive",
      )
    }

    // SigV4 region validation
    if (awsSigningEnabled) {
      if (awsRegion.isEmpty) {
        throw new ConfigException(
          "connect.opensearch.aws.region is required when aws.signing.enabled=true",
        )
      }
      val knownRegions = Region.regions().asScala.map(_.id()).toSeq
      if (!knownRegions.contains(awsRegion)) {
        throw new ConfigException(
          s"connect.opensearch.aws.region='$awsRegion' is not a known AWS region; expected one of ${knownRegions.sorted.mkString(", ")}",
        )
      }

      // SigV4 service validation
      if (!Set("es", "aoss").contains(awsSigningService)) {
        throw new ConfigException(
          s"connect.opensearch.aws.signing.service must be one of {es, aoss}; got '$awsSigningService'",
        )
      }

      // aws.credentials.provider must be one of the known enum values
      val knownProviders = Set("DEFAULT", "STATIC")
      if (!knownProviders.contains(awsCredProvider)) {
        throw new ConfigException(
          s"connect.opensearch.aws.credentials.provider='$awsCredProvider' is not a valid value; " +
            s"expected one of {${knownProviders.toSeq.sorted.mkString(", ")}}",
        )
      }

      // STATIC credentials require keys
      if (awsCredProvider == "STATIC") {
        if (awsAccessKeyId.isEmpty) {
          throw new ConfigException(
            "connect.opensearch.aws.access.key.id is required when aws.credentials.provider=STATIC",
          )
        }
        if (awsSecretAccessKey.isEmpty) {
          throw new ConfigException(
            "connect.opensearch.aws.secret.access.key is required when aws.credentials.provider=STATIC",
          )
        }
      }

      // SigV4 host cardinality: exactly one host
      if (hostsRaw.size != 1) {
        throw new ConfigException(
          s"connect.opensearch.aws.signing.enabled=true requires exactly one entry in connect.opensearch.hosts; got ${hostsRaw.size}",
        )
      }

      // Host must not contain a scheme
      val h = hostsRaw.head
      if (h.startsWith("http://") || h.startsWith("https://")) {
        throw new ConfigException(
          s"connect.opensearch.hosts must be a bare hostname when aws.signing.enabled=true; got '$h' — strip the scheme",
        )
      }

      // SigV4 requires HTTPS — signing over plain HTTP exposes credentials in clear text.
      // 'http' (the config default) is also rejected here; operators must set protocol=https explicitly.
      if (protocol != "https") {
        throw new ConfigException(
          s"connect.opensearch.protocol must be 'https' when aws.signing.enabled=true; got '$protocol'. " +
            "Signing over HTTP exposes credentials — set connect.opensearch.protocol=https.",
        )
      }
    }

    // Reject cleartext auth: protocol=http with Basic or JWT credentials is insecure unless the
    // operator explicitly acknowledges the risk via ALLOW_INSECURE_AUTH_KEY.
    val allowInsecureAuth = config.getBoolean(ALLOW_INSECURE_AUTH_KEY)
    val hasBasicAuth      = username.nonEmpty || password.nonEmpty
    val hasJwt            = jwtToken.nonEmpty || jwtTokenFile.nonEmpty
    if (!awsSigningEnabled && protocol != "https" && (hasBasicAuth || hasJwt) && !allowInsecureAuth) {
      val mech = if (hasBasicAuth) "HTTP basic auth" else "JWT bearer authentication"
      throw new ConfigException(
        s"connect.opensearch.protocol='$protocol' would send $mech credentials over cleartext HTTP. " +
          s"Set connect.opensearch.protocol=https, or if TLS is terminated by a trusted local sidecar " +
          s"explicitly set $ALLOW_INSECURE_AUTH_KEY=true.",
      )
    }
    if (allowInsecureAuth && protocol != "https" && (hasBasicAuth || hasJwt)) {
      logger.warn(
        s"$ALLOW_INSECURE_AUTH_KEY=true — sending authentication credentials over $protocol. " +
          "Ensure TLS is terminated by a trusted local sidecar/mesh; otherwise rotate credentials immediately.",
      )
    }

    // Warn: protocol=http with TLS material
    val sslKeystoreLocation = Try(Option(config.getString("ssl.keystore.location"))).toOption.flatten.filter(_.nonEmpty)
    val sslTruststoreLocation =
      Try(Option(config.getString("ssl.truststore.location"))).toOption.flatten.filter(_.nonEmpty)
    if (protocol == "http" && (sslKeystoreLocation.nonEmpty || sslTruststoreLocation.nonEmpty) && !awsSigningEnabled) {
      logger.warn(
        s"connect.opensearch.protocol=http but TLS material is configured " +
          s"(ssl.keystore.location=${sslKeystoreLocation.getOrElse("")} / " +
          s"ssl.truststore.location=${sslTruststoreLocation.getOrElse("")}); " +
          s"the SSLContext built from StoresInfo will NOT be used because the request scheme is HTTP. " +
          s"Set connect.opensearch.protocol=https to enable TLS.",
      )
    }

    // AWS session token startup warning
    if (awsSessionToken.nonEmpty) {
      logger.warn(
        "AWS session token configured via connect.opensearch.aws.session.token; " +
          "this token does NOT auto-refresh and the connector will fail when it expires. " +
          "Use connect.opensearch.aws.credentials.provider=DEFAULT (instance profile / IAM role / web identity) for production deployments.",
      )
    }

    val storesInfo = unpackOrThrow(StoresInfo.fromConfig(config))

    val kcqls = config.getString(KCQL).split(";").filter(_.trim.nonEmpty).map(io.lenses.kcql.Kcql.parse).toIndexedSeq

    val common = ElasticCommonSettings(
      kcqls                 = kcqls,
      errorPolicy           = config.getErrorPolicy,
      taskRetries           = config.getNumberRetries,
      writeTimeout          = config.getWriteTimeout,
      batchSize             = config.getInt(BATCH_SIZE),
      pkJoinerSeparator     = config.getString(PK_JOINER_SEPARATOR),
      httpBasicAuthUsername = username,
      httpBasicAuthPassword = password,
      storesInfo            = storesInfo,
      strictItemErrors      = strictItemErrors,
    )

    val jwtTokenSource =
      if (jwtToken.nonEmpty) {
        Some(JwtTokenSource.fromStaticToken(jwtToken))
      } else if (jwtTokenFile.nonEmpty) {
        Some(JwtTokenSource.fromFile(jwtTokenFile, jwtRefreshMs, jwtBaseDir))
      } else {
        None
      }

    val maxConnectionsPerRoute = config.getInt(MAX_CONNECTIONS_PER_ROUTE_KEY)
    val maxConnectionsTotal    = config.getInt(MAX_CONNECTIONS_TOTAL_KEY)

    OpenSearchSettings(
      common                 = common,
      hosts                  = hostsRaw,
      port                   = port,
      tablePrefix            = tablePrefix,
      protocol               = protocol,
      awsSigningEnabled      = awsSigningEnabled,
      awsRegion              = awsRegion,
      awsSigningService      = awsSigningService,
      awsCredentialsProvider = awsCredProvider,
      awsAccessKeyId         = awsAccessKeyId,
      awsSecretAccessKey     = awsSecretAccessKey,
      awsSessionToken        = awsSessionToken,
      jwtTokenSource         = jwtTokenSource,
      strictItemErrors       = strictItemErrors,
      maxConnectionsPerRoute = maxConnectionsPerRoute,
      maxConnectionsTotal    = maxConnectionsTotal,
    )
  }
}

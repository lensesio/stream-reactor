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
package io.lenses.streamreactor.connect.cassandra

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Field
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorPolicy
import org.apache.kafka.common.config.ConfigException

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

sealed trait IgnoredErrorMode
object IgnoredErrorMode {
  case object None   extends IgnoredErrorMode
  case object All    extends IgnoredErrorMode
  case object Driver extends IgnoredErrorMode

  def fromString(value: String): IgnoredErrorMode = value.toLowerCase match {
    case "none"   => None
    case "all"    => All
    case "driver" => Driver
    case _        => throw new ConfigException(s"Unknown ignored error type: $value")
  }
}

case class CassandraSSLSettings(
  provider:             Option[String],
  trustStorePath:       Option[String],
  trustStorePass:       Option[String],
  keyStorePath:         Option[String],
  keyStorePass:         Option[String],
  cipherSuites:         Option[String],
  hostnameVerification: Boolean,
  openSslKeyCertChain:  Option[String],
  openSslPrivateKey:    Option[String],
)

case class CassandraAuthSettings(
  provider:        String,
  username:        Option[String],
  password:        Option[String],
  gssapiKeyTab:    Option[String],
  gssapiPrincipal: Option[String],
  gssapiService:   String,
)

case class CassandraTableSettingFieldMapping(
  from:      String,
  to:        String,
  fieldType: String,
)

case class CassandraTableSetting(
  namespace:        String,
  table:            String,
  sourceKafkaTopic: String,
  fieldMappings:    Seq[CassandraTableSettingFieldMapping],
  others:           Map[String, String],
)

object CassandraTableSetting {
  private val CassandraNamePattern = "^[a-zA-Z][a-zA-Z0-9_]{0,47}$"

  /**
   * Create the topic mapping
   * @param name
   * @param kind
   * @return
   */
  def getTopicMappings(setting: CassandraTableSetting): Option[(String, String)] =
    if (setting.fieldMappings.nonEmpty) {
      val fieldsMapping =
        setting.fieldMappings
          .map { mapping =>
            if (mapping.from.toLowerCase() == "_header") {
              throw new ConfigException(
                s"Invalid field name 'header': field names must be 'key', 'value', or start with 'key.' or 'value.' or 'header.', or be one of supported functions: '[now()]'",
              )
            }
            val from = mapping.fieldType match {
              case "value"  => mapping.from.stripPrefix("_value").stripPrefix(".")
              case "key"    => mapping.from.stripPrefix("_key").stripPrefix(".")
              case "header" => mapping.from.stripPrefix("_header").stripPrefix(".")
              case _        => mapping.from
            }

            val isFunction = from.contains("(") || from.contains(")")
            s""""${mapping.to}"="${if (!isFunction) mapping.fieldType else ""}${(if (from.trim.nonEmpty && !isFunction)
                                                                                   "."
                                                                                 else "") + from}"""".stripMargin
          }
          .mkString(", ")
      Some(s"topic.${setting.sourceKafkaTopic}.${setting.namespace}.${setting.table}.mapping" -> fieldsMapping)
    } else {
      None
    }

  def getExtraSettingsMappings(setting: CassandraTableSetting): Map[String, String] =
    setting.others.map { case (key, value) =>
      if (key.startsWith("codec.")) {
        s"topic.${setting.sourceKafkaTopic}.$key" -> value
      } else {
        s"topic.${setting.sourceKafkaTopic}.${setting.namespace}.${setting.table}.$key" -> value
      }
    }

  private def validateCassandraName(name: String, kind: String): String = {
    if (!name.matches(CassandraNamePattern))
      throw new ConfigException(
        s"Invalid Cassandra $kind name: '$name'. Must start with a letter, contain only letters, numbers, and underscores, and be at most 48 characters.",
      )
    name
  }

  private def createFieldMapping(field: Field, fieldType: String): CassandraTableSettingFieldMapping = {
    val alias        = if (field.getAlias.startsWith("'")) field.getAlias.stripPrefix("'").stripSuffix("'") else field.getAlias
    val adaptedField = field.toString.replaceAll("'", "")
    CassandraTableSettingFieldMapping(adaptedField, alias, fieldType)
  }

  private def extractFieldMappings(kcql: Kcql): Seq[CassandraTableSettingFieldMapping] = {
    val fieldTypes = Seq(
      (kcql.getFields.asScala, "value"),
      (kcql.getKeyFields.asScala, "key"),
      (kcql.getHeaderFields.asScala, "header"),
    )

    fieldTypes.flatMap { case (fields, fieldType) =>
      fields.filterNot(_.getName == "*").map(createFieldMapping(_, fieldType))
    }
  }

  def fromKcql(kcql: Kcql): CassandraTableSetting = {
    val target = kcql.getTarget
    val (namespaceRaw, tableRaw) = target.split("\\.", 2) match {
      case Array(ns, tbl) => (ns, tbl)
      case _              => throw new ConfigException(s"KCQL target must be in the form namespace.table, got: $target")
    }

    val namespace        = validateCassandraName(namespaceRaw, "keyspace (namespace)")
    val table            = validateCassandraName(tableRaw, "table")
    val sourceKafkaTopic = kcql.getSource
    val fieldMappings: Seq[CassandraTableSettingFieldMapping] = extractFieldMappings(kcql)
    val others = kcql.getProperties.asScala.toMap

    CassandraTableSetting(namespace, table, sourceKafkaTopic, fieldMappings, others)
  }
}

case class CassandraSettings(
  connectorName:         String,
  tableSettings:         Seq[CassandraTableSetting],
  errorPolicy:           ErrorPolicy,
  taskRetries:           Int     = CassandraConfigConstants.NBR_OF_RETIRES_DEFAULT,
  enableProgress:        Boolean = CassandraConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
  maxConcurrentRequests: Int,
  connectionPoolSize:    Int,
  compression:           String,
  queryTimeout:          Int,
  maxBatchSize:          Int,
  localDc:               Option[String],
  contactPoints:         Seq[String],
  port:                  Int,
  auth:                  Option[CassandraAuthSettings],
  ssl:                   Option[CassandraSSLSettings],
  ignoredError:          IgnoredErrorMode,
  driverSettings:        Map[String, String],
)

object CassandraSettings extends StrictLogging {

  private def getOptionalString(config: CassandraConfig, key: String): Option[String] =
    Option(config.getString(key)).filter(_.nonEmpty)

  private def getOptionalPassword(config: CassandraConfig, key: String): Option[String] =
    Option(config.getPassword(key)).map(_.value).filter(_.nonEmpty)

  private def extractAuthSettings(config: CassandraConfig): Option[CassandraAuthSettings] = {
    val authProvider = config.getString(CassandraConfigConstants.AUTH_PROVIDER)

    Option.when(authProvider != "None") {
      CassandraAuthSettings(
        authProvider,
        getOptionalString(config, CassandraConfigConstants.AUTH_USERNAME),
        getOptionalPassword(config, CassandraConfigConstants.AUTH_PASSWORD),
        getOptionalString(config, CassandraConfigConstants.AUTH_GSSAPI_KEYTAB),
        getOptionalString(config, CassandraConfigConstants.AUTH_GSSAPI_PRINCIPAL),
        config.getString(CassandraConfigConstants.AUTH_GSSAPI_SERVICE),
      )
    }
  }

  private def extractSSLSettings(config: CassandraConfig): Option[CassandraSSLSettings] =
    Option.when(config.getBoolean(CassandraConfigConstants.SSL_ENABLED)) {
      CassandraSSLSettings(
        getOptionalString(config, CassandraConfigConstants.SSL_PROVIDER),
        getOptionalString(config, CassandraConfigConstants.SSL_TRUST_STORE_PATH),
        getOptionalPassword(config, CassandraConfigConstants.SSL_TRUST_STORE_PASSWD),
        getOptionalString(config, CassandraConfigConstants.SSL_KEY_STORE_PATH),
        getOptionalPassword(config, CassandraConfigConstants.SSL_KEY_STORE_PASSWD),
        getOptionalString(config, CassandraConfigConstants.SSL_CIPHER_SUITES),
        config.getBoolean(CassandraConfigConstants.SSL_HOSTNAME_VERIFICATION),
        getOptionalString(config, CassandraConfigConstants.SSL_OPENSSL_KEY_CERT_CHAIN),
        getOptionalString(config, CassandraConfigConstants.SSL_OPENSSL_PRIVATE_KEY),
      )
    }

  private def extractContactPoints(config: CassandraConfig): Seq[String] =
    config.getString(CassandraConfigConstants.CONTACT_POINTS)
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSeq

  def getOSSCassandraSinkConfig(settings: CassandraSettings): Map[String, String] = {
    val baseConfig = Map(
      "name"                                                  -> settings.connectorName,
      "contactPoints"                                         -> settings.contactPoints.mkString(","),
      "port"                                                  -> settings.port.toString,
      "maxConcurrentRequests"                                 -> settings.maxConcurrentRequests.toString,
      "connectionPoolLocalSize"                               -> settings.connectionPoolSize.toString,
      "compression"                                           -> settings.compression,
      "queryExecutionTimeout"                                 -> settings.queryTimeout.toString,
      "maxNumberOfRecordsInBatch"                             -> settings.maxBatchSize.toString,
      "ignoreErrors"                                          -> settings.ignoredError.toString.toLowerCase,
      "datastax-java-driver.basic.request.timeout"            -> s"${settings.queryTimeout} seconds",
      "datastax-java-driver.basic.connection.pool.local.size" -> settings.connectionPoolSize.toString,
    )

    val localDcConfig = settings.localDc.map("loadBalancing.localDc" -> _).toMap

    val sslConfig = settings.ssl.map { ssl =>
      val sslMappings = Seq(
        ssl.provider.map("ssl.provider"                        -> _),
        ssl.trustStorePath.map("ssl.truststore.path"           -> _),
        ssl.trustStorePass.map("ssl.truststore.password"       -> _),
        ssl.keyStorePath.map("ssl.keystore.path"               -> _),
        ssl.keyStorePass.map("ssl.keystore.password"           -> _),
        ssl.cipherSuites.map("ssl.cipherSuites"                -> _),
        Some("ssl.hostnameValidation"                          -> ssl.hostnameVerification.toString),
        ssl.openSslKeyCertChain.map("ssl.openssl.keyCertChain" -> _),
        ssl.openSslPrivateKey.map("ssl.openssl.privateKey"     -> _),
      )
      sslMappings.flatten.toMap
    }.getOrElse(Map.empty)

    val authConfig = settings.auth.map { auth =>
      val authMappings = Seq(
        Some("auth.provider"                             -> auth.provider),
        Some("auth.gssapi.service"                       -> auth.gssapiService),
        auth.username.map("auth.username"                -> _),
        auth.password.map("auth.password"                -> _),
        auth.gssapiKeyTab.map("auth.gssapi.keyTab"       -> _),
        auth.gssapiPrincipal.map("auth.gssapi.principal" -> _),
      )
      authMappings.flatten.toMap
    }.getOrElse(Map.empty)

    val topicMappings = settings.tableSettings.foldLeft(Map.empty[String, String]) { (acc, setting) =>
      acc ++ CassandraTableSetting.getTopicMappings(setting).fold(Map.empty[String, String])(
        Map(_),
      ) ++ CassandraTableSetting.getExtraSettingsMappings(setting)
    }
    val configMap = baseConfig ++ localDcConfig ++ sslConfig ++ authConfig ++ settings.driverSettings ++ topicMappings
    logger.info(
      s"""
         |Cassandra OSS Sink Config:
         |${configMap.map { case (k, v) => s"$k = $v" }.mkString("\n")}
         |""".stripMargin,
    )
    configMap
  }

  def configureSink(config: CassandraConfig): CassandraSettings = {

    val contactPoints = extractContactPoints(config)
    if (contactPoints.isEmpty) {
      throw new ConfigException("contact.points must be set")
    }

    CassandraSettings(
      connectorName         = config.props.getOrElse(CassandraConfigConstants.CONNECTOR_NAME_KEY, "unknown-connector"),
      tableSettings         = config.getKCQL.toSeq.map(CassandraTableSetting.fromKcql),
      errorPolicy           = config.getErrorPolicy,
      taskRetries           = config.getNumberRetries,
      enableProgress        = config.getBoolean(CassandraConfigConstants.PROGRESS_COUNTER_ENABLED),
      maxConcurrentRequests = config.getInt(CassandraConfigConstants.MAX_CONCURRENT_REQUESTS),
      connectionPoolSize    = config.getInt(CassandraConfigConstants.CONNECTION_POOL_SIZE),
      compression           = config.getString(CassandraConfigConstants.COMPRESSION),
      queryTimeout          = config.getInt(CassandraConfigConstants.QUERY_TIMEOUT),
      maxBatchSize          = config.getInt(CassandraConfigConstants.MAX_BATCH_SIZE),
      localDc               = getOptionalString(config, CassandraConfigConstants.LOAD_BALANCING_LOCAL_DC),
      contactPoints         = contactPoints,
      port                  = config.getInt(CassandraConfigConstants.PORT),
      auth                  = extractAuthSettings(config),
      ssl                   = extractSSLSettings(config),
      ignoredError = IgnoredErrorMode.fromString(
        config.getString(CassandraConfigConstants.IGNORE_ERRORS_MODE).toLowerCase,
      ),
      driverSettings = config.props.view.filterKeys(_.startsWith("connect.cassandra.driver."))
        .map { case (key, value) =>
          "datastax-java-driver." + key.stripPrefix("connect.cassandra.driver.") -> value
        }.toMap,
    )
  }

}

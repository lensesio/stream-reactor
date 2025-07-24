package io.lenses.streamreactor.connect.test

import _root_.io.lenses.streamreactor.connect.testcontainers.connect._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef

trait HttpConfiguration extends LazyLogging {

  val ERROR_REPORTING_ENABLED_PROP   = "connect.reporting.error.config.enabled"
  val SUCCESS_REPORTING_ENABLED_PROP = "connect.reporting.success.config.enabled"

  val ERROR_REPORTING_TOPIC_PROP   = "connect.reporting.error.config.topic"
  val SUCCESS_REPORTING_TOPIC_PROP = "connect.reporting.success.config.topic"

  val ERROR_REPORTING_BOOTSTRAP_SERVERS_PROP   = "connect.reporting.error.config.bootstrap.servers"
  val SUCCESS_REPORTING_BOOTSTRAP_SERVERS_PROP = "connect.reporting.success.config.bootstrap.servers"

  def sinkConfig(
    randomTestId:          String,
    endpointUrl:           String,
    httpMethod:            String,
    contentTemplate:       String,
    headerTemplates:       Seq[(String, String)],
    topicName:             String,
    converters:            Map[String, String],
    batchSize:             Int,
    jsonTidy:              Boolean,
    errorReportingTopic:   String,
    successReportingTopic: String,
    bootstrapServers:      String,
    copyMessageHeaders:    Boolean = false,
  ): ConnectorConfiguration = {
    val configMap: Map[String, ConfigValue[_]] = converters.view.mapValues(new ConfigValue[String](_)).toMap ++
      Map(
        "connector.class"                        -> ConfigValue("io.lenses.streamreactor.connect.http.sink.HttpSinkConnector"),
        "tasks.max"                              -> ConfigValue(1),
        "topics"                                 -> ConfigValue(topicName),
        HttpSinkConfigDef.HttpMethodProp         -> ConfigValue(HttpMethod.withNameInsensitive(httpMethod).toString),
        HttpSinkConfigDef.HttpEndpointProp       -> ConfigValue(endpointUrl),
        HttpSinkConfigDef.HttpRequestContentProp -> ConfigValue(contentTemplate),
        HttpSinkConfigDef.HttpRequestHeadersProp -> ConfigValue(headerTemplates.mkString(",")),
        HttpSinkConfigDef.AuthenticationTypeProp -> ConfigValue("none"), //NoAuthentication
        HttpSinkConfigDef.BatchCountProp         -> ConfigValue(batchSize),
        HttpSinkConfigDef.BatchSizeProp          -> ConfigValue(100_000_000),
        HttpSinkConfigDef.TimeIntervalProp       -> ConfigValue(100_000_000),
        HttpSinkConfigDef.JsonTidyProp           -> ConfigValue(jsonTidy),
        ERROR_REPORTING_ENABLED_PROP             -> ConfigValue("true"),
        ERROR_REPORTING_TOPIC_PROP               -> ConfigValue(errorReportingTopic),
        ERROR_REPORTING_BOOTSTRAP_SERVERS_PROP   -> ConfigValue(bootstrapServers),
        SUCCESS_REPORTING_ENABLED_PROP           -> ConfigValue("true"),
        SUCCESS_REPORTING_TOPIC_PROP             -> ConfigValue(successReportingTopic),
        SUCCESS_REPORTING_BOOTSTRAP_SERVERS_PROP -> ConfigValue(bootstrapServers),
        HttpSinkConfigDef.RetriesMaxRetriesProp  -> ConfigValue(0),
        HttpSinkConfigDef.CopyMessageHeadersProp -> ConfigValue(copyMessageHeaders),
      )
    debugLogConnectorConfig(configMap)
    ConnectorConfiguration(
      "connector" + randomTestId,
      configMap,
    )
  }

  private def debugLogConnectorConfig(configMap: Map[String, ConfigValue[_]]): Unit = {
    logger.debug("Creating connector with configuration:")
    configMap.foreachEntry {
      case (k, v) => logger.debug(s"    $k => ${v.underlying}")
    }
    logger.debug(s"End connector config.")
  }
}

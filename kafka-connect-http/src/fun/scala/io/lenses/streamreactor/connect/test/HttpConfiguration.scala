package io.lenses.streamreactor.connect.test

import _root_.io.lenses.streamreactor.connect.testcontainers.connect._
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config.BatchConfiguration
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig

trait HttpConfiguration extends LazyLogging {

  def sinkConfig(
    randomTestId:    String,
    endpointUrl:     String,
    httpMethod:      String,
    contentTemplate: String,
    headerTemplates: Seq[(String, String)],
    topicName:       String,
    converters:      Map[String, String],
  ): ConnectorConfiguration = {
    val configMap: Map[String, ConfigValue[_]] = converters.view.mapValues(new ConfigValue[String](_)).toMap ++
      Map(
        "connector.class" -> ConfigValue("io.lenses.streamreactor.connect.http.sink.HttpSinkConnector"),
        "tasks.max"       -> ConfigValue(1),
        "topics"          -> ConfigValue(topicName),
        "connect.http.config" -> ConfigValue(
          HttpSinkConfig(
            Option.empty,
            HttpMethod.withNameInsensitive(httpMethod),
            endpoint         = endpointUrl,
            content          = contentTemplate,
            headers          = headerTemplates,
            sslConfig        = Option.empty,
            batch            = Option(BatchConfiguration(1L.some, none, none)),
            errorThreshold   = Option.empty,
            uploadSyncPeriod = Option.empty,
          ).toJson,
        ),
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

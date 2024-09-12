package io.lenses.streamreactor.connect.test

import _root_.io.lenses.streamreactor.connect.testcontainers.connect._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.{HttpMethod, NoAuthentication}
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef

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
        HttpSinkConfigDef.HttpMethodProp -> ConfigValue(HttpMethod.withNameInsensitive(httpMethod)),
        HttpSinkConfigDef.HttpEndpointProp -> ConfigValue(endpointUrl),
        HttpSinkConfigDef.HttpRequestContentProp -> ConfigValue(contentTemplate),
        HttpSinkConfigDef.HttpRequestHeadersProp -> ConfigValue(headerTemplates),
        HttpSinkConfigDef.AuthenticationTypeProp -> ConfigValue(NoAuthentication),
        HttpSinkConfigDef.BatchCountProp -> ConfigValue(2),

//        "connect.http.config" -> ConfigValue(
//          HttpSinkConfig(
//            HttpMethod.withNameInsensitive(httpMethod),
//            endpoint = endpointUrl,
//            content  = contentTemplate,
//            NoAuthentication,
//            headers          = headerTemplates.toList,
//            ssl              = Option.empty,
//            batch            = BatchConfig(1L.some, none, none),
//            errorThreshold   = 0,
//            uploadSyncPeriod = 100,
//            retries = RetriesConfig(3, 200, List(200)),
//            timeout = TimeoutConfig(500),
//          ).toJson,
//        ),
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

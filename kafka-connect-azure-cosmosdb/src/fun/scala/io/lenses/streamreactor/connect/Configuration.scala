package io.lenses.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.connect._

object Configuration {

  def sinkConfig(
    randomTestId:     String,
    masterKey:             String,
    networkAliasUrl:  String,
    databaseName:       String,
    prefix:           String,
    topicName:        String,
  ): ConnectorConfiguration =
    ConnectorConfiguration(
      "connector" + randomTestId,
      Map(
        "connector.class"            -> ConfigValue("io.lenses.streamreactor.connect.azure.cosmosdb.sink.AzureCosmosDbSinkConnector"),
        "tasks.max"                  -> ConfigValue(1),
        "topics"                     -> ConfigValue(topicName),
        "connect.azure.cosmosdb.master.key"  -> ConfigValue(masterKey),
        "connect.azure.cosmosdb.endpoint" -> ConfigValue(networkAliasUrl),
        "connect.azure.cosmosdb.kcql" -> ConfigValue(
          s"INSERT INTO `$databaseName:$prefix` SELECT * FROM `$topicName` PROPERTIES('flush.count'=1)",
        ),
      ),
    )
}

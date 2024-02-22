package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

import io.lenses.java.streamreactor.common.config.base.BaseConfig;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class AzureEventHubsConfig extends BaseConfig {

  public static final String CONNECTION_GROUP = "Connection";
  @Getter
  static ConfigDef configDefinition = new ConfigDef()
      //from LC-107
      .define(AzureEventHubsConfigConstants.CONSUMER_GROUP,
          Type.STRING,
          Importance.HIGH,
          AzureEventHubsConfigConstants.CONSUMER_GROUP_DOC,
          CONNECTION_GROUP,
          1,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.CONSUMER_GROUP
      ).define(AzureEventHubsConfigConstants.NAMESPACE,
          Type.STRING,
          Importance.HIGH,
          AzureEventHubsConfigConstants.NAMESPACE_DOC,
          CONNECTION_GROUP,
          2,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.NAMESPACE
      ).define(AzureEventHubsConfigConstants.EVENTHUB_NAME,
          Type.STRING,
          Importance.HIGH,
          AzureEventHubsConfigConstants.EVENTHUB_NAME_DOC,
          CONNECTION_GROUP,
          3,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.EVENTHUB_NAME
      ).define(AzureEventHubsConfigConstants.CONNECTION_STRING,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.HIGH,
          AzureEventHubsConfigConstants.CONNECTION_STRING_DOC,
          CONNECTION_GROUP,
          4,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.CONNECTION_STRING
      ).define(AzureEventHubsConfigConstants.USERNAME,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.MEDIUM,
          AzureEventHubsConfigConstants.USERNAME_DOC,
          CONNECTION_GROUP,
          5,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.USERNAME
      ).define(AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE,
          Type.STRING,
          AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE_DEFAULT,
          Importance.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE_DOC,
          CONNECTION_GROUP,
          6,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE
      ).define(AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL_DOC,
          CONNECTION_GROUP,
          7,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL
      ).define(AzureEventHubsConfigConstants.SCHEMA_GROUP,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_GROUP_DOC,
          CONNECTION_GROUP,
          8,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.SCHEMA_GROUP
      ).define(AzureEventHubsConfigConstants.CLIENT_ID,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.HIGH,
          AzureEventHubsConfigConstants.CLIENT_ID_DOC,
          CONNECTION_GROUP,
          9,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.CLIENT_ID
      ).define(AzureEventHubsConfigConstants.CLIENT_SECRET,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.HIGH,
          AzureEventHubsConfigConstants.CLIENT_SECRET_DOC,
          CONNECTION_GROUP,
          10,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.CLIENT_SECRET
      ).define(AzureEventHubsConfigConstants.TENANT_ID,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.LOW,
          AzureEventHubsConfigConstants.TENANT_ID_DOC,
          CONNECTION_GROUP,
          11,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.TENANT_ID
      ).define(AzureEventHubsConfigConstants.USER_INFO,
          Type.STRING,
          AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
          Importance.LOW,
          AzureEventHubsConfigConstants.USER_INFO_DOC,
          CONNECTION_GROUP,
          12,
          ConfigDef.Width.MEDIUM,
          AzureEventHubsConfigConstants.USER_INFO
      ).define(AzureEventHubsConfigConstants.KCQL_CONFIG,
          Type.STRING,
          AzureEventHubsConfigConstants.KCQL_DEFAULT,
          Importance.HIGH,
          AzureEventHubsConfigConstants.KCQL_DOC,
          "Mappings",
          1,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.KCQL_CONFIG
      ).define(AzureEventHubsConfigConstants.INCLUDE_HEADERS,
          Type.BOOLEAN,
          AzureEventHubsConfigConstants.INCLUDE_HEADERS_DEFAULT,
          Importance.MEDIUM,
          AzureEventHubsConfigConstants.INCLUDE_HEADERS_DOC,
          "Mappings",
          2,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.INCLUDE_HEADERS
      ).define(
          AzureEventHubsConfigConstants.NBR_OF_RETRIES_CONFIG,
          Type.INT,
          AzureEventHubsConfigConstants.NBR_OF_RETRIES_CONFIG_DEFAULT,
          Importance.LOW,
          AzureEventHubsConfigConstants.NBR_OF_RETRIES_CONFIG_DOC,
          "Error",
          1,
          ConfigDef.Width.LONG,
          AzureEventHubsConfigConstants.NBR_OF_RETRIES_CONFIG
      );

  public AzureEventHubsConfig(Map<?, ?> properties) {
    super(AzureEventHubsConfigConstants.CONNECTOR_PREFIX, getConfigDefinition(), properties);
  }
}

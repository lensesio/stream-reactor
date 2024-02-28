package io.lenses.java.streamreactor.connect.azure.eventhubs.config;

import io.lenses.java.streamreactor.common.config.base.BaseConfig;
import io.lenses.java.streamreactor.common.config.base.intf.ConnectorPrefixed;
import java.util.Map;
import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

/**
 * Class represents Config Definition for AzureEventHubsSourceConnector. It additionally adds
 * configs from org.apache.kafka.clients.consumer.ConsumerConfig but adds standard Connector
 * prefixes to them.
 */
public class AzureEventHubsConfig extends BaseConfig implements ConnectorPrefixed {

  private static final String DOT = ".";

  public static final String CONNECTION_GROUP = "Connection";

  private static final UnaryOperator<String> CONFIG_NAME_PREFIX_APPENDER = name ->
      AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX + DOT + name;


  @Getter
  static ConfigDef configDefinition;

  static {
    ConfigDef kafkaConsumerConfigToExpose = getKafkaConsumerConfigToExpose();
    configDefinition = new ConfigDef(kafkaConsumerConfigToExpose)
        .define(AzureEventHubsConfigConstants.EVENTHUB_NAME,
            Type.STRING,
            Importance.HIGH,
            AzureEventHubsConfigConstants.EVENTHUB_NAME_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.EVENTHUB_NAME
            //CONFIGS BELOW IDENTIFIED AS NOT NEEDED (for now)
            //).define(AzureEventHubsConfigConstants.CONNECTION_STRING,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.HIGH,
            //    AzureEventHubsConfigConstants.CONNECTION_STRING_DOC,
            //    CONNECTION_GROUP,
            //    2,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.CONNECTION_STRING
            //).define(AzureEventHubsConfigConstants.USERNAME,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.MEDIUM,
            //    AzureEventHubsConfigConstants.USERNAME_DOC,
            //    CONNECTION_GROUP,
            //    3,
            //    ConfigDef.Width.LONG,
            //    AzureEventHubsConfigConstants.USERNAME
            //).define(AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE_DEFAULT,
            //    Importance.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE_DOC,
            //    CONNECTION_GROUP,
            //    4,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_REGISTRY_TYPE
            //).define(AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL_DOC,
            //    CONNECTION_GROUP,
            //    5,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_REGISTRY_URL
            //).define(AzureEventHubsConfigConstants.SCHEMA_GROUP,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_GROUP_DOC,
            //    CONNECTION_GROUP,
            //    6,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.SCHEMA_GROUP
            //).define(AzureEventHubsConfigConstants.CLIENT_ID,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.HIGH,
            //    AzureEventHubsConfigConstants.CLIENT_ID_DOC,
            //    CONNECTION_GROUP,
            //    7,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.CLIENT_ID
            //).define(AzureEventHubsConfigConstants.CLIENT_SECRET,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.HIGH,
            //    AzureEventHubsConfigConstants.CLIENT_SECRET_DOC,
            //    CONNECTION_GROUP,
            //    8,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.CLIENT_SECRET
            //).define(AzureEventHubsConfigConstants.TENANT_ID,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.LOW,
            //    AzureEventHubsConfigConstants.TENANT_ID_DOC,
            //    CONNECTION_GROUP,
            //    9,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.TENANT_ID
            //).define(AzureEventHubsConfigConstants.USER_INFO,
            //    Type.STRING,
            //    AzureEventHubsConfigConstants.OPTIONAL_EMPTY_DEFAULT,
            //    Importance.LOW,
            //    AzureEventHubsConfigConstants.USER_INFO_DOC,
            //    CONNECTION_GROUP,
            //    10,
            //    ConfigDef.Width.MEDIUM,
            //    AzureEventHubsConfigConstants.USER_INFO
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
  }

  public AzureEventHubsConfig(Map<?, ?> properties) {
    super(AzureEventHubsConfigConstants.CONNECTOR_PREFIX, getConfigDefinition(), properties);
  }

  /**
   * Provides prefixed KafkaConsumerConfig key.
   *
   * @param kafkaConsumerConfigKey from org.apache.kafka.clients.consumer.ConsumerConfig
   * @return prefixed key.
   */
  public static String getPrefixedKafkaConsumerConfigKey(String kafkaConsumerConfigKey) {
    return CONFIG_NAME_PREFIX_APPENDER.apply(kafkaConsumerConfigKey);
  }

  @Override
  public String connectorPrefix() {
    return AzureEventHubsConfigConstants.CONNECTOR_PREFIX;
  }

  private static ConfigDef getKafkaConsumerConfigToExpose() {
    ConfigDef kafkaConsumerConfigToExpose = new ConfigDef();
    ConsumerConfig.configDef().configKeys().values()
        .forEach(configKey -> kafkaConsumerConfigToExpose.define(
            CONFIG_NAME_PREFIX_APPENDER.apply(configKey.name),
            configKey.type, configKey.defaultValue,
            configKey.importance, configKey.documentation, configKey.group,
            configKey.orderInGroup, configKey.width, configKey.displayName));

    return kafkaConsumerConfigToExpose;
  }
}

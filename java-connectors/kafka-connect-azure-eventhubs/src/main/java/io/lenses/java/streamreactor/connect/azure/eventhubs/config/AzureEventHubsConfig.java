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
      AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX + name;


  @Getter
  static ConfigDef configDefinition;

  static {
    ConfigDef kafkaConsumerConfigToExpose = getKafkaConsumerConfigToExpose();
    configDefinition = new ConfigDef(kafkaConsumerConfigToExpose)
        .define(AzureEventHubsConfigConstants.CONNECTOR_NAME,
            Type.STRING,
            AzureEventHubsConfigConstants.CONNECTOR_NAME_DEFAULT,
            Importance.HIGH,
            AzureEventHubsConfigConstants.CONNECTOR_NAME_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.CONNECTOR_NAME
        )
        .define(AzureEventHubsConfigConstants.EVENTHUB_NAME,
            Type.STRING,
            Importance.HIGH,
            AzureEventHubsConfigConstants.EVENTHUB_NAME_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.EVENTHUB_NAME
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

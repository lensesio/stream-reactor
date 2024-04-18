/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.eventhubs.config;

import io.lenses.streamreactor.common.config.base.BaseConfig;
import io.lenses.streamreactor.common.config.base.intf.ConnectorPrefixed;
import java.util.Map;
import java.util.Set;
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
public class AzureEventHubsSourceConfig extends BaseConfig implements ConnectorPrefixed {

  public static final String CONNECTION_GROUP = "Connection";

  private static final UnaryOperator<String> CONFIG_NAME_PREFIX_APPENDER = name ->
      AzureEventHubsConfigConstants.CONNECTOR_WITH_CONSUMER_PREFIX + name;

  private static final Set<String> EXCLUDED_CONSUMER_PROPERTIES =
      Set.of(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ConsumerConfig.GROUP_ID_CONFIG, ConsumerConfig.CLIENT_ID_CONFIG, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);


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
        ).define(AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT,
            Type.INT,
            AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT_DEFAULT,
            Importance.MEDIUM,
            AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.CONSUMER_CLOSE_TIMEOUT
        )
        .define(AzureEventHubsConfigConstants.CONSUMER_OFFSET,
            Type.STRING,
            AzureEventHubsConfigConstants.CONSUMER_OFFSET_DEFAULT,
            Importance.MEDIUM,
            AzureEventHubsConfigConstants.CONSUMER_OFFSET_DOC,
            CONNECTION_GROUP,
            4,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.CONSUMER_OFFSET
        ).define(AzureEventHubsConfigConstants.KCQL_CONFIG,
            Type.STRING,
            Importance.HIGH,
            AzureEventHubsConfigConstants.KCQL_DOC,
            "Mappings",
            1,
            ConfigDef.Width.LONG,
            AzureEventHubsConfigConstants.KCQL_CONFIG
        );
  }

  public AzureEventHubsSourceConfig(Map<?, ?> properties) {
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
    return connectorPrefix;
  }

  private static ConfigDef getKafkaConsumerConfigToExpose() {
    ConfigDef kafkaConsumerConfigToExpose = new ConfigDef();
    ConsumerConfig.configDef().configKeys().values().stream()
        .filter(configKey -> !EXCLUDED_CONSUMER_PROPERTIES.contains(configKey.name))
        .forEach(configKey -> kafkaConsumerConfigToExpose.define(
            CONFIG_NAME_PREFIX_APPENDER.apply(configKey.name),
            configKey.type, configKey.defaultValue,
            configKey.importance, configKey.documentation, configKey.group,
            configKey.orderInGroup, configKey.width, configKey.displayName));

    return kafkaConsumerConfigToExpose;
  }
}

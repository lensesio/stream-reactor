/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.servicebus.config;

import io.lenses.streamreactor.common.config.base.BaseConfig;
import io.lenses.streamreactor.common.config.base.intf.ConnectorPrefixed;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

/**
 * Service Bus Connector Configuration class.
 */
public class AzureServiceBusSourceConfig extends BaseConfig implements ConnectorPrefixed {

  @Getter
  private static final ConfigDef configDefinition;

  public static final String CONNECTION_GROUP = "Connection";
  public static final String BASE_GROUP = "Base";

  static {
    configDefinition =
        new ConfigDef()
            .define(AzureServiceBusConfigConstants.CONNECTOR_NAME,
                Type.STRING,
                AzureServiceBusConfigConstants.CONNECTOR_NAME_DEFAULT,
                Importance.HIGH,
                AzureServiceBusConfigConstants.CONNECTOR_NAME_DOC,
                BASE_GROUP,
                1,
                ConfigDef.Width.LONG,
                AzureServiceBusConfigConstants.CONNECTOR_NAME
            ).define(AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE,
                Type.INT,
                AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE_DEFAULT,
                Importance.MEDIUM,
                AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE_DOC,
                BASE_GROUP,
                2,
                Width.SHORT,
                AzureServiceBusConfigConstants.TASK_RECORDS_QUEUE_SIZE
            ).define(AzureServiceBusConfigConstants.CONNECTION_STRING,
                Type.STRING,
                Importance.HIGH,
                AzureServiceBusConfigConstants.CONNECTION_STRING_DOC,
                CONNECTION_GROUP,
                2,
                ConfigDef.Width.LONG,
                AzureServiceBusConfigConstants.CONNECTION_STRING
            ).define(AzureServiceBusConfigConstants.KCQL_CONFIG,
                Type.STRING,
                Importance.HIGH,
                AzureServiceBusConfigConstants.KCQL_DOC,
                "Mappings",
                1,
                ConfigDef.Width.LONG,
                AzureServiceBusConfigConstants.KCQL_CONFIG
            );
  }

  public AzureServiceBusSourceConfig(Map<?, ?> properties) {
    super(AzureServiceBusConfigConstants.CONNECTOR_PREFIX, getConfigDefinition(), properties);
  }

  @Override
  public String connectorPrefix() {
    return connectorPrefix;
  }

}

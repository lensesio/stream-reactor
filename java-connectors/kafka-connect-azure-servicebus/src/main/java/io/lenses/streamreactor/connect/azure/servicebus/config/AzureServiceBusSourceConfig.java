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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.lenses.streamreactor.common.config.base.BaseConfig;
import io.lenses.streamreactor.common.config.base.intf.ConnectorPrefixed;
import lombok.Getter;

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
                AzureServiceBusConfigConstants.SOURCE_CONNECTOR_NAME_DEFAULT,
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
                Type.PASSWORD,
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
            )
            .define(AzureServiceBusConfigConstants.SOURCE_PREFETCH_COUNT,
                Type.INT,
                AzureServiceBusConfigConstants.SOURCE_PREFETCH_COUNT_DEFAULT,
                Importance.MEDIUM,
                AzureServiceBusConfigConstants.SOURCE_PREFETCH_COUNT_DOC,
                BASE_GROUP,
                2,
                Width.SHORT,
                AzureServiceBusConfigConstants.SOURCE_PREFETCH_COUNT
            )
            .define(AzureServiceBusConfigConstants.SOURCE_MAX_COMPLETE_RETRIES,
                Type.INT,
                AzureServiceBusConfigConstants.SOURCE_MAX_COMPLETE_RETRIES_DEFAULT,
                Importance.MEDIUM,
                AzureServiceBusConfigConstants.SOURCE_MAX_COMPLETE_RETRIES_DOC,
                BASE_GROUP,
                3,
                Width.SHORT,
                AzureServiceBusConfigConstants.SOURCE_MAX_COMPLETE_RETRIES
            )
            .define(AzureServiceBusConfigConstants.SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS,
                Type.INT,
                AzureServiceBusConfigConstants.SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS_DEFAULT,
                Importance.MEDIUM,
                AzureServiceBusConfigConstants.SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS_DOC,
                BASE_GROUP,
                4,
                Width.SHORT,
                AzureServiceBusConfigConstants.SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS
            )
            .define(AzureServiceBusConfigConstants.SOURCE_SLEEP_ON_EMPTY_POLL_MS,
                Type.LONG,
                AzureServiceBusConfigConstants.SOURCE_SLEEP_ON_EMPTY_POLL_MS_DEFAULT,
                Importance.MEDIUM,
                AzureServiceBusConfigConstants.SOURCE_SLEEP_ON_EMPTY_POLL_MS_DOC,
                BASE_GROUP,
                5,
                Width.SHORT,
                AzureServiceBusConfigConstants.SOURCE_SLEEP_ON_EMPTY_POLL_MS
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

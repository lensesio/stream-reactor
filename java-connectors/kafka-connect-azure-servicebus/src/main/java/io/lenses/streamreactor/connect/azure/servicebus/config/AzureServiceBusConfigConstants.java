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

import io.lenses.streamreactor.connect.azure.servicebus.source.AzureServiceBusSourceConnector;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Class to indicate String constants used in Service Bus Connectors Config.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AzureServiceBusConfigConstants {

  private static final String DOT = ".";
  public static final String CONNECTOR_PREFIX = "connect.servicebus";
  public static final String SOURCE_CONNECTOR_PREFIX = CONNECTOR_PREFIX + DOT + "source";

  public static final String CONNECTOR_NAME = "name";
  public static final String CONNECTOR_NAME_DOC = "Connector's name";
  public static final String CONNECTOR_NAME_DEFAULT = AzureServiceBusSourceConnector.class.getSimpleName();
  public static final String CONNECTION_STRING = CONNECTOR_PREFIX + DOT + "connection.string";
  public static final String CONNECTION_STRING_DOC = "Azure ServiceBus Connection String";
  public static final String KCQL_CONFIG = CONNECTOR_PREFIX + DOT + "kcql";
  public static final String KCQL_DOC =
      "KCQL expression describing field selection and data routing to the target.";

  public static final String TASK_RECORDS_QUEUE_SIZE = SOURCE_CONNECTOR_PREFIX + "task.records.queue.size";
  public static final String TASK_RECORDS_QUEUE_SIZE_DOC = "Task's records queue size.";
  public static final int TASK_RECORDS_QUEUE_SIZE_DEFAULT = 20;

}

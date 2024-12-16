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

import io.lenses.streamreactor.connect.azure.servicebus.sink.AzureServiceBusSinkConnector;
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
  public static final String SINK_CONNECTOR_PREFIX = CONNECTOR_PREFIX + DOT + "sink";

  public static final String CONNECTOR_NAME = "name";
  public static final String CONNECTOR_NAME_DOC = "Connector's name";
  public static final String SOURCE_CONNECTOR_NAME_DEFAULT = AzureServiceBusSourceConnector.class.getSimpleName();
  public static final String SINK_CONNECTOR_NAME_DEFAULT = AzureServiceBusSinkConnector.class.getSimpleName();
  public static final String CONNECTION_STRING = CONNECTOR_PREFIX + DOT + "connection.string";
  public static final String CONNECTION_STRING_DOC = "Azure ServiceBus Connection String";
  public static final String KCQL_CONFIG = CONNECTOR_PREFIX + DOT + "kcql";
  public static final String KCQL_DOC =
      "KCQL expression describing field selection and data routing to the target.";

  public static final String TASK_RECORDS_QUEUE_SIZE = SOURCE_CONNECTOR_PREFIX + DOT + "task.records.queue.size";
  public static final String TASK_RECORDS_QUEUE_SIZE_DOC =
      "An internal buffer that accumulates records arriving asynchronously from ServiceBus.";

  public static final int TASK_RECORDS_QUEUE_SIZE_DEFAULT = 5000;

  public static final String SOURCE_PREFETCH_COUNT_DOC =
      "The number of messages to prefetch from the Azure Service Bus.";
  public static final String SOURCE_PREFETCH_COUNT = SOURCE_CONNECTOR_PREFIX + DOT + "prefetch.count";
  public static final int SOURCE_PREFETCH_COUNT_DEFAULT = 1000;

  public static final String SOURCE_MAX_COMPLETE_RETRIES_DOC = "The maximum number of retries to complete a message.";
  public static final String SOURCE_MAX_COMPLETE_RETRIES = SOURCE_CONNECTOR_PREFIX + DOT + "complete.retries.max";
  public static final int SOURCE_MAX_COMPLETE_RETRIES_DEFAULT = 3;

  public static final String SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS_DOC =
      "The minimum duration in milliseconds for the first backoff";
  public static final String SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS =
      SOURCE_CONNECTOR_PREFIX + DOT + "complete.retries.min.backoff.ms";
  public static final int SOURCE_MIN_BACKOFF_COMPLETE_RETRIES_MS_DEFAULT = 1000;

  public static final String SOURCE_SLEEP_ON_EMPTY_POLL_MS_DOC =
      "The duration in milliseconds to sleep when no records are returned from the poll. This avoids a tight loop in Connect.";
  public static final String SOURCE_SLEEP_ON_EMPTY_POLL_MS =
      SOURCE_CONNECTOR_PREFIX + DOT + "sleep.on.empty.poll.ms";
  public static final int SOURCE_SLEEP_ON_EMPTY_POLL_MS_DEFAULT = 250;

  public static final String MAX_NUMBER_OF_RETRIES = SINK_CONNECTOR_PREFIX + DOT + "retries.max";
  public static final String MAX_NUMBER_OF_RETRIES_DOC = "Maximum number of retries if message sending fails.";
  public static final int MAX_NUMBER_OF_RETRIES_DEFAULT = 3;

  public static final String TIMEOUT_BETWEEN_RETRIES = SINK_CONNECTOR_PREFIX + DOT + "retries.timeout";
  public static final String TIMEOUT_BETWEEN_RETRIES_DOC = "Timeout (in millis) between retries.";
  public static final int TIMEOUT_BETWEEN_RETRIES_DEFAULT = 500;

}

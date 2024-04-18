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

import io.lenses.streamreactor.connect.azure.eventhubs.source.AzureEventHubsSourceConnector;

/**
 * Class represents Config Constants for AzureEventHubsSourceConnector Config Definition.
 */
public class AzureEventHubsConfigConstants {


  private static final String DOT = ".";
  public static final String OPTIONAL_EMPTY_DEFAULT = "";
  public static final String CONNECTOR_PREFIX = "connect.eventhubs";
  public static final String SOURCE_CONNECTOR_PREFIX = CONNECTOR_PREFIX + DOT + "source";

  public static final String CONNECTOR_NAME = "name";
  public static final String CONNECTOR_NAME_DOC = "Connector's name";
  public static final String CONNECTOR_NAME_DEFAULT = AzureEventHubsSourceConnector.class.getSimpleName();

  public static final String CONNECTOR_WITH_CONSUMER_PREFIX =
      SOURCE_CONNECTOR_PREFIX + DOT + "connection.settings" + DOT;
  public static final String CONSUMER_OFFSET = SOURCE_CONNECTOR_PREFIX + DOT + "default.offset";
  public static final String CONSUMER_OFFSET_DOC =
      "Specifies whether by default we should consumer from earliest (default) or latest offset.";
  public static final String CONSUMER_OFFSET_DEFAULT = "earliest";
  public static final String CONSUMER_CLOSE_TIMEOUT = SOURCE_CONNECTOR_PREFIX + DOT + "close.timeout";
  public static final String CONSUMER_CLOSE_TIMEOUT_DOC =
      "Specifies timeout for consumer closing.";
  public static final String CONSUMER_CLOSE_TIMEOUT_DEFAULT = "30";

  public static final String KCQL_CONFIG = CONNECTOR_PREFIX + DOT + "kcql";
  public static final String KCQL_DOC =
      "KCQL expression describing field selection and data routing to the target.";

}

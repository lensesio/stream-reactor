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
package io.lenses.streamreactor.connect.azure.servicebus.util;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents one of two types of Azure Service Bus: Queue or Topic.
 */
public enum ServiceBusType {

  QUEUE, TOPIC;

  private static final Map<String, ServiceBusType> STRING_TO_VALUE =
      Arrays.stream(ServiceBusType.values())
          .collect(Collectors.toMap(Enum::name, v -> v));

  /**
   * Gets relevant type of Service Bus for case-insensitive string.
   * 
   * @param value case-insensitive String.
   */
  public static ServiceBusType fromString(String value) {
    return STRING_TO_VALUE.get(value.toUpperCase());
  }
}

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
import java.util.List;
import java.util.stream.Collectors;

import lombok.Getter;

/**
 * This class represents valid values that can be put in PROPERTIES section of KCQL for Service Bus Connector Mappings.
 */
@Getter
public enum ServiceBusKcqlProperties {

  SERVICE_BUS_TYPE(PropertiesConstants.SERVICE_BUS_TYPE_PROP, PropertiesConstants.SERVICE_BUS_TYPE_DESC, false),
  BATCH_ENABLED(PropertiesConstants.BATCH_ENABLED_TYPE_PROP, PropertiesConstants.BATCH_ENABLED_TYPE_DESC, true),
  SUBSCRIPTION_NAME(PropertiesConstants.SUBSCRIPTION_NAME_PROP, PropertiesConstants.SUBSCRIPTION_NAME_DESC, true);

  private final String propertyName;
  private final String description;
  private final boolean optional;

  ServiceBusKcqlProperties(String propertyName, String description, boolean optional) {
    this.propertyName = propertyName;
    this.description = description;
    this.optional = optional;
  }

  /**
   * Returns list of non-optional KCQL Properties for ServiceBus.
   */
  public static List<ServiceBusKcqlProperties> getNecessaryProperties() {
    return Arrays.stream(ServiceBusKcqlProperties.values()).filter(property -> !property.isOptional())
        .collect(Collectors.toList());
  }

  private static class PropertiesConstants {

    private static final String SERVICE_BUS_TYPE_PROP = "servicebus.type";
    private static final String SERVICE_BUS_TYPE_DESC = "ServiceBus Type: QUEUE/TOPIC";
    private static final String SUBSCRIPTION_NAME_PROP = "subscription.name";
    private static final String SUBSCRIPTION_NAME_DESC = "ServiceBus subscription name";
    private static final String BATCH_ENABLED_TYPE_PROP = "batch.enabled";
    private static final String BATCH_ENABLED_TYPE_DESC = "Batching messages disabled";
  }
}

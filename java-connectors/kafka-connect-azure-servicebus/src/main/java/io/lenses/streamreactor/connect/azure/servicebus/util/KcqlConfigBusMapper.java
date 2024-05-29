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

import io.lenses.kcql.Kcql;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.config.ConfigException;

/**
 * Class that represents methods around KCQL topic handling.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KcqlConfigBusMapper {

  private static final String AZURE_NAME_REGEX = "^[A-Za-z0-9]$|^[A-Za-z0-9][\\w-\\.\\/\\~]*[A-Za-z0-9]$";
  private static final String BUS_TYPE_REGEX = "^TOPIC$|^QUEUE$";
  private static final String QUEUE_BUS_TYPE = "QUEUE";
  private static final int MAX_BUS_NAME_LENGTH = 160;
  private static final int MAX_SUBSCRIPTION_LENGTH = 50;
  private static final Pattern AZURE_NAME_PATTERN = Pattern.compile(AZURE_NAME_REGEX);
  private static final Pattern BUS_TYPE_PATTERN = Pattern.compile(BUS_TYPE_REGEX);
  private static final String TOPIC_NAME_ERROR_MESSAGE =
      "%s topic %s, name is not correctly specified: It can contain only letters, numbers and hyphens,"
          + " underscores and dots and has to start with number or letter with max size of " + MAX_BUS_NAME_LENGTH;

  private static final String SUBSCRIPTION_NAME_ERROR_MESSAGE =
      "Subscription name %s is not correctly specified but is necessary for TOPIC: "
          + "It can contain only letters, numbers and hyphens, underscores and dots and "
          + "has to start with number or letter with max size of " + MAX_SUBSCRIPTION_LENGTH;

  private static final String BUS_TYPE_ERROR_MESSAGE =
      "Property %s contains invalid value. Valid values are: TOPIC or QUEUE";
  private static final List<ServiceBusKcqlProperties> NECESSARY_PROPERTIES =
      ServiceBusKcqlProperties.getNecessaryProperties();

  /**
   * This method parses KCQL statements and fetches input and output topics checking against regex for invalid
   * topic names in input and output as well as if mappings contain necessary KCQL properties.
   * 
   * @param kcqlString string to parse
   * @return list of KCQLs if parsed properly
   */
  public static List<Kcql> mapKcqlsFromConfig(String kcqlString) {
    List<Kcql> kcqls = Kcql.parseMultiple(kcqlString);
    Map<String, String> inputToOutputTopics = new HashMap<>(kcqls.size());

    for (Kcql kcql : kcqls) {
      validateTopicNames(kcql);
      validateTopicMappings(inputToOutputTopics, kcql);
      validateKcqlProperties(kcql);

      inputToOutputTopics.put(kcql.getSource(), kcql.getTarget());
    }

    return kcqls;
  }

  private static void validateTopicNames(Kcql kcql) {
    String inputTopic = kcql.getSource();
    String outputTopic = kcql.getTarget();

    if (!azureNameMatchesAgainstRegex(inputTopic, MAX_BUS_NAME_LENGTH)) {
      throw new ConfigException(String.format(TOPIC_NAME_ERROR_MESSAGE, "Input", inputTopic));
    }
    if (!azureNameMatchesAgainstRegex(outputTopic, MAX_BUS_NAME_LENGTH)) {
      throw new ConfigException(String.format(TOPIC_NAME_ERROR_MESSAGE, "Output", outputTopic));
    }
  }

  public static void validateTopicMappings(Map<String, String> inputToOutputTopics, Kcql kcql) {
    String inputTopic = kcql.getSource();
    String outputTopic = kcql.getTarget();

    if (inputToOutputTopics.containsKey(inputTopic)) {
      throw new ConfigException(String.format("Input '%s' cannot be mapped twice.", inputTopic));
    }
    if (inputToOutputTopics.containsValue(outputTopic)) {
      throw new ConfigException(String.format("Output '%s' cannot be mapped twice.", outputTopic));
    }
  }

  public static void validateKcqlProperties(Kcql kcql) {
    List<ServiceBusKcqlProperties> notSatisfiedProperties = checkForNecessaryKcqlProperties(kcql);
    if (!notSatisfiedProperties.isEmpty()) {
      String missingPropertiesError =
          notSatisfiedProperties.stream()
              .map(ServiceBusKcqlProperties::getPropertyName)
              .collect(Collectors.joining(","));
      throw new ConfigException(String.format("Following non-optional properties are missing in KCQL: %s",
          missingPropertiesError));
    }

    checkForValidPropertyValues(kcql.getProperties());
  }

  private static void checkForValidPropertyValues(Map<String, String> properties) {
    String serviceBusType = properties.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName()).toUpperCase();
    if (!BUS_TYPE_PATTERN.matcher(serviceBusType).matches()) {
      throw new ConfigException(
          String.format(BUS_TYPE_ERROR_MESSAGE, ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName()));
    }

    if (!QUEUE_BUS_TYPE.equalsIgnoreCase(serviceBusType)) { // if not a queue, check for necessary topic subscription
      String subscriptionName = properties.get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName());
      if (subscriptionName == null || !azureNameMatchesAgainstRegex(subscriptionName, MAX_SUBSCRIPTION_LENGTH)) {
        throw new ConfigException(String.format(SUBSCRIPTION_NAME_ERROR_MESSAGE, subscriptionName));
      }
    }
  }

  private static List<ServiceBusKcqlProperties> checkForNecessaryKcqlProperties(Kcql kcql) {
    Map<String, String> kcqlProperties = kcql.getProperties();
    return NECESSARY_PROPERTIES.stream()
        .filter(property -> !kcqlProperties.containsKey(property.getPropertyName())).collect(Collectors.toList());
  }

  private static boolean azureNameMatchesAgainstRegex(String name, int length) {
    final Matcher matcher = AZURE_NAME_PATTERN.matcher(name);
    return matcher.matches() && name.length() < length;
  }
}

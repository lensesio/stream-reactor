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

import static io.lenses.streamreactor.common.util.StringUtils.getSystemsNewLineChar;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import io.lenses.kcql.Kcql;
import lombok.*;

/**
 * Class that represents methods around KCQL topic handling.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KcqlConfigBusMapper {

  private static final String AZURE_NAME_REGEX = "^[A-Za-z0-9]$|^[A-Za-z0-9][\\w-\\.\\/\\~]*[A-Za-z0-9]$";
  private static final String BUS_TYPE_REGEX = "^TOPIC$|^QUEUE$";
  private static final String QUEUE_BUS_TYPE = "QUEUE";
  private static final String ERROR_DELIMITER = ";" + getSystemsNewLineChar();
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

    val inputTopics = kcqls.stream().map(Kcql::getSource).collect(Collectors.toList());
    val outputTopics = kcqls.stream().map(Kcql::getTarget).collect(Collectors.toList());

    val allErrors =
        Stream.of(
            validateTopicMappings(inputTopics, "Input"),
            validateTopicMappings(outputTopics, "Output"),
            validateTopicName(inputTopics, "Input"),
            validateTopicName(outputTopics, "Output"),
            kcqls.stream().flatMap(KcqlConfigBusMapper::validateKcqlProperties)
        ).flatMap(Function.identity())
            .collect(Collectors.toUnmodifiableSet());

    if (!allErrors.isEmpty()) {
      throw new ConfigException("The following errors occurred during validation: ", String.join(ERROR_DELIMITER,
          allErrors));
    }

    return List.copyOf(kcqls);
  }

  private static Stream<String> validateTopicName(List<String> topicNames, String description) {
    return topicNames.stream()
        .filter(topicName -> !azureNameMatchesAgainstRegex(topicName, MAX_BUS_NAME_LENGTH))
        .map(topicName -> String.format(TOPIC_NAME_ERROR_MESSAGE, description, topicName));

  }

  private static Stream<String> detectDuplicateTopicMappings(List<String> topics) {
    return topics.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(Map.Entry::getKey);
  }

  private static Stream<String> validateTopicMappings(List<String> topics, String description) {
    return detectDuplicateTopicMappings(topics)
        .map(
            dupe -> String.format("%s '%s' cannot be mapped twice.", description, dupe)
        );
  }

  private static Stream<String> validateKcqlProperties(Kcql kcql) {
    return Stream.concat(
        validateNecessaryKcqlProperties(kcql),
        checkForValidPropertyValues(kcql.getProperties())
    );
  }

  private static Stream<String> validateNecessaryKcqlProperties(Kcql kcql) {
    return Optional.ofNullable(findUndefiniedNecessaryKcqlProperties(kcql))
        .filter(notSatisfiedProperties -> !notSatisfiedProperties.isEmpty())
        .map(notSatisfiedProperties -> notSatisfiedProperties.stream()
            .map(ServiceBusKcqlProperties::getPropertyName)
            .collect(Collectors.joining(",")))
        .map(missingPropertiesError -> new String(
            String.format("Following non-optional properties are missing in KCQL: %s", missingPropertiesError)
        )).stream();
  }

  public static Stream<String> checkForValidPropertyValues(Map<String, String> properties) {

    Optional<String> serviceBusType =
        Optional.ofNullable(properties.get(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName())).map(
            String::toUpperCase);

    return serviceBusType.map(sbt -> {

      Stream.Builder<String> errorStreamBuilder = Stream.builder();
      if (!BUS_TYPE_PATTERN.matcher(sbt).matches()) {
        errorStreamBuilder.add(
            String.format(BUS_TYPE_ERROR_MESSAGE, ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName()));
      }
      if (!QUEUE_BUS_TYPE.equalsIgnoreCase(sbt)) { // if not a queue, check for necessary topic subscription
        Optional<String> subscriptionName =
            Optional.ofNullable(properties.get(ServiceBusKcqlProperties.SUBSCRIPTION_NAME.getPropertyName()));
        if (subscriptionName.isEmpty() || subscriptionName.stream().filter(e -> azureNameMatchesAgainstRegex(e,
            MAX_SUBSCRIPTION_LENGTH)).collect(Collectors.toSet()).isEmpty()) {
          errorStreamBuilder.add(String.format(SUBSCRIPTION_NAME_ERROR_MESSAGE,
              subscriptionName.orElse(null)));
        }
      }
      return errorStreamBuilder.build();

    }

    ).orElse(Stream.empty());

  }

  private static List<ServiceBusKcqlProperties> findUndefiniedNecessaryKcqlProperties(Kcql kcql) {
    Map<String, String> kcqlProperties = kcql.getProperties();
    return NECESSARY_PROPERTIES.stream()
        .filter(property -> !kcqlProperties.containsKey(property.getPropertyName())).collect(Collectors.toList());
  }

  private static boolean azureNameMatchesAgainstRegex(String name, int length) {
    final Matcher matcher = AZURE_NAME_PATTERN.matcher(name);
    return matcher.matches() && name.length() < length;
  }
}

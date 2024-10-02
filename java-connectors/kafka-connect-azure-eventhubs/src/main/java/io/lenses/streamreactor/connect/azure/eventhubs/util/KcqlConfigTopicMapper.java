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
package io.lenses.streamreactor.connect.azure.eventhubs.util;

import static io.lenses.streamreactor.common.util.StringUtils.getSystemsNewLineChar;

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.ConnectorStartupException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Class that represents methods around KCQL topic handling.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KcqlConfigTopicMapper {

  private static final String TOPIC_NAME_REGEX = "^[\\w][\\w\\-\\_\\.]*$";
  private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile(TOPIC_NAME_REGEX);

  private static final String ERROR_DELIMITER = ";" + getSystemsNewLineChar();
  public static final String TOPIC_NAME_ERROR_MESSAGE =
      "%s topic %s, name is not correctly specified (It can contain only letters, numbers and hyphens,"
          + " underscores and dots and has to start with number or letter)";

  /**
   * This method parses KCQL statements and fetches input and output topics checking against regex for invalid topic
   * names in input and output.
   *
   * @param kcqlString string to parse
   * @return map of input to output topic names
   */
  public static Either<ConnectorStartupException, List<Kcql>> mapInputToOutputsFromConfig(String kcqlString) {
    List<Kcql> kcqls = Kcql.parseMultiple(kcqlString);

    List<String> inputTopics = kcqls.stream().map(Kcql::getSource).collect(Collectors.toUnmodifiableList());
    List<String> outputTopics = kcqls.stream().map(Kcql::getTarget).collect(Collectors.toUnmodifiableList());

    Set<String> allErrors =
        Stream.of(
            validateTopicMappings(inputTopics, "Input"),
            validateTopicMappings(outputTopics, "Output"),
            validateTopicName(inputTopics, "Input"),
            validateTopicName(outputTopics, "Output")
        ).flatMap(Collection::stream).collect(Collectors.toUnmodifiableSet());

    if (!allErrors.isEmpty()) {
      return Either.left(new ConnectorStartupException(
          String.format("The following errors occurred during validation: %s", String.join(ERROR_DELIMITER,
              allErrors))));
    }

    return Either.right(List.copyOf(kcqls));
  }

  private static List<String> validateTopicName(List<String> topicNames, String description) {
    return topicNames.stream()
        .filter(topicName -> !topicNameMatchesAgainstRegex(topicName))
        .map(topicName -> String.format(TOPIC_NAME_ERROR_MESSAGE, description, topicName))
        .collect(Collectors.toUnmodifiableList());
  }

  private static List<String> detectDuplicateTopicMappings(List<String> topics) {
    return topics.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableList());
  }

  private static List<String> validateTopicMappings(List<String> topics, String description) {
    return detectDuplicateTopicMappings(topics).stream()
        .map(
            dupe -> String.format("%s '%s' cannot be mapped twice.", description, dupe)
        ).collect(Collectors.toUnmodifiableList());
  }

  private static boolean topicNameMatchesAgainstRegex(String topicName) {
    final Matcher matcher = TOPIC_NAME_PATTERN.matcher(topicName);
    return matcher.matches();
  }
}

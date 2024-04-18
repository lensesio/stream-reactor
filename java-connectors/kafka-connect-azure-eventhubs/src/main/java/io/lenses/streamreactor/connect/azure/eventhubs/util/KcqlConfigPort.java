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
package io.lenses.streamreactor.connect.azure.eventhubs.util;

import io.lenses.kcql.Kcql;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigException;

/**
 * Class that represents methods around KCQL handling.
 */
public class KcqlConfigPort {

  private static final String TOPIC_NAME_REGEX = "^[\\w][\\w\\-\\_\\.]*$";
  private static final Pattern TOPIC_NAME_PATTERN = Pattern.compile(TOPIC_NAME_REGEX);
  public static final String TOPIC_NAME_ERROR_MESSAGE =
      "%s topic %s, name is not correctly specified (It can contain only letters, numbers and hyphens,"
          + " underscores and dots and has to start with number or letter";

  /**
   * This method parses KCQL statements and fetches input and output topics checking against
   * regex for invalid topic names in input and output.
   * @param kcqlString string to parse
   * @return map of input to output topic names
   */
  public static Map<String, String> mapInputToOutputsFromConfig(String kcqlString) {
    List<Kcql> kcqls = Kcql.parseMultiple(kcqlString);
    Map<String, String> inputToOutputTopics = new HashMap<>(kcqls.size());
    List<String> outputTopics = new ArrayList<>(kcqls.size());

    for (Kcql kcql : kcqls) {
      String inputTopic = kcql.getSource();
      String outputTopic = kcql.getTarget();

      if (!topicNameMatchesAgainstRegex(inputTopic)) {
        throw new ConfigException(String.format(TOPIC_NAME_ERROR_MESSAGE, "Input", inputTopic));
      }
      if (!topicNameMatchesAgainstRegex(outputTopic)) {
        throw new ConfigException(String.format(TOPIC_NAME_ERROR_MESSAGE, "Output", outputTopic));
      }
      if (inputToOutputTopics.containsKey(inputTopic)) {
        throw new ConfigException(String.format("Input %s cannot be mapped twice.", inputTopic));
      }
      if (outputTopics.contains(outputTopic)) {
        throw new ConfigException(String.format("Output %s cannot be mapped twice.", outputTopic));
      }

      inputToOutputTopics.put(inputTopic, outputTopic);
      outputTopics.add(outputTopic);
    }

    return inputToOutputTopics;
  }

  private static boolean topicNameMatchesAgainstRegex(String topicName) {
    final Matcher matcher = TOPIC_NAME_PATTERN.matcher(topicName);
    return matcher.matches();
  }
}

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

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.common.validation.ConnectorConfigKcqlValidator;
import io.lenses.streamreactor.common.validation.validators.DistinctSourceNamesValidator;
import io.lenses.streamreactor.common.validation.validators.DistinctTargetNamesValidator;
import io.lenses.streamreactor.common.validation.validators.PatternMatchingSourceNameValidator;
import io.lenses.streamreactor.common.validation.validators.PatternMatchingTargetNameValidator;
import java.util.List;
import java.util.function.UnaryOperator;

public class EventHubsKcqlMappingsValidator {

  private static final String TOPIC_NAME_REGEX = "^[\\w][\\w\\-\\_\\.]*$";

  public static final String TOPIC_NAME_ERROR_MESSAGE =
      "%s topic %s, name is not correctly specified (It can contain only letters, numbers and hyphens,"
          + " underscores and dots and has to start with number or letter)";

  public static final UnaryOperator<String> OUTPUT_TOPIC_ERROR_MESSAGE_FUNCTION =
      topicName -> String.format(TOPIC_NAME_ERROR_MESSAGE, "Output", topicName);

  public static final UnaryOperator<String> INPUT_TOPIC_ERROR_MESSAGE_FUNCTION =
      topicName -> String.format(TOPIC_NAME_ERROR_MESSAGE, "Input", topicName);

  private static final ConnectorConfigKcqlValidator EVENT_HUBS_KCQL_VALIDATOR =
      ConnectorConfigKcqlValidator.builder()
          .singleKcqlValidators(List.of(
              new PatternMatchingSourceNameValidator(TOPIC_NAME_REGEX, INPUT_TOPIC_ERROR_MESSAGE_FUNCTION),
              new PatternMatchingTargetNameValidator(TOPIC_NAME_REGEX, OUTPUT_TOPIC_ERROR_MESSAGE_FUNCTION)
          ))
          .allKcqlValidators(List.of(
              new DistinctSourceNamesValidator(),
              new DistinctTargetNamesValidator()
          ))
          .build();

  /**
   * This method parses KCQL statements and fetches input and output topics checking against regex for invalid topic
   * names in input and output.
   *
   * @param kcqlString string to parse
   * @return map of input to output topic names
   */
  public static Either<StreamReactorException, List<Kcql>> mapInputToOutputsFromConfig(String kcqlString) {
    return EVENT_HUBS_KCQL_VALIDATOR.validateKcqlString(kcqlString);
  }

}

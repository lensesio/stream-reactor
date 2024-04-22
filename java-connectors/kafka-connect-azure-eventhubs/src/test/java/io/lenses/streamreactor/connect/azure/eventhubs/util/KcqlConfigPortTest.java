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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class KcqlConfigPortTest {

  @Test
  void mapInputToOutputsFromConfigForMultipleKcqlStatementsShouldRetunMapOfInputToOutput() {
    //given
    int numberOfMappings = 3;
    List<String> inputs = new ArrayList<>(numberOfMappings);
    List<String> outputs = new ArrayList<>(numberOfMappings);
    String kcqlTemplate = "insert into %s select * from %s;";
    StringBuilder fullKcql = new StringBuilder();

    for (int i = 0; i < numberOfMappings; i++) {
      String newInput = "INPUT" + i;
      String newOutput = "OUTPUT" + i;

      inputs.add(i, newInput);
      outputs.add(i, newOutput);
      fullKcql.append(String.format(kcqlTemplate, newOutput, newInput));
    }
    //when
    Map<String, String> inputToOutputsFromConfig = KcqlConfigPort.mapInputToOutputsFromConfig(
        fullKcql.toString());

    //then
    for (String input : inputToOutputsFromConfig.keySet()){
      int indexOfInput = inputs.indexOf(input);
      assertNotEquals(-1, indexOfInput);
      assertEquals(inputs.get(indexOfInput), input);
      assertEquals(outputs.get(indexOfInput), inputToOutputsFromConfig.get(input));
    }
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForIllegalNames() {
    //given
    String illegalInputKcql = "INSERT INTO OUTPUT SELECT * FROM 'INPUT*_'";
    String illegalOutputKcql = "INSERT INTO 'OUTPUT*_' SELECT * FROM INPUT";
    String inputErrorMessage = "Input topic INPUT*_, name is not correctly specified "
        + "(It can contain only letters, numbers and hyphens, underscores and "
        + "dots and has to start with number or letter";
    String outputErrorMessage = "Output topic OUTPUT*_, name is not correctly specified "
        + "(It can contain only letters, numbers and hyphens, underscores and "
        + "dots and has to start with number or letter";

    //when
    mapInputToOutputAddertingExceptionWithSpecificMessage(illegalInputKcql, inputErrorMessage);

    mapInputToOutputAddertingExceptionWithSpecificMessage(illegalOutputKcql, outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForOneToManyMappings() {
    //given
    String oneInputKcql = "INSERT INTO OUTPUT1 SELECT * FROM INPUT1;";
    String sameInputKcql = "INSERT INTO OUTPUT2 SELECT * FROM INPUT1;";
    String outputErrorMessage = "Input INPUT1 cannot be mapped twice.";

    //when
    mapInputToOutputAddertingExceptionWithSpecificMessage(oneInputKcql + sameInputKcql,
        outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForMiltipleInputsToSameOutput() {
    //given
    String oneInputKcql = "INSERT INTO OUTPUT1 SELECT * FROM INPUT1;";
    String anotherInputToSameOutputKcql = "INSERT INTO OUTPUT1 SELECT * FROM INPUT2;";
    String outputErrorMessage = "Output OUTPUT1 cannot be mapped twice.";

    //when
    mapInputToOutputAddertingExceptionWithSpecificMessage(
        oneInputKcql + anotherInputToSameOutputKcql,
        outputErrorMessage);
  }

  private static void mapInputToOutputAddertingExceptionWithSpecificMessage(String illegalKcql,
      String expectedMessage) {
    assertThrows(ConfigException.class,
        () -> KcqlConfigPort.mapInputToOutputsFromConfig(illegalKcql), expectedMessage);
  }
}
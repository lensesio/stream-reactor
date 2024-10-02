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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.ConnectorStartupException;
import io.lenses.streamreactor.test.utils.EitherValues;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class KcqlConfigTopicMapperTest {

  @Test
  void mapInputToOutputsFromConfigForMultipleKcqlStatementsShouldReturnMapOfInputToOutput() {
    //given
    int numberOfMappings = 3;
    List<String> inputs = new ArrayList<>(numberOfMappings);
    List<String> outputs = new ArrayList<>(numberOfMappings);
    String kcqlTemplate = "insert into %s select * from %s";
    StringBuilder fullKcql = new StringBuilder();

    for (int i = 0; i < numberOfMappings; i++) {
      String newInput = "INPUT" + i;
      String newOutput = "OUTPUT" + i;

      inputs.add(i, newInput);
      outputs.add(i, newOutput);
      fullKcql.append(String.format(kcqlTemplate, newOutput, newInput)).append(";");
    }
    //when
    List<Kcql> kcqls =
        EitherValues.getRight(KcqlConfigTopicMapper.mapInputToOutputsFromConfig(
            fullKcql.toString()));

    //then
    for (int i = 0; i < numberOfMappings; i++) {
      Kcql kcql = kcqls.get(i);
      assertThat(kcql)
          .returns(inputs.get(i), from(Kcql::getSource))
          .returns(outputs.get(i), from(Kcql::getTarget));
    }
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForIllegalNames() {
    //given
    String illegalInputKcql =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT*_';";
    String illegalOutputKcql =
        "INSERT INTO 'OUTPUT*_' SELECT * FROM INPUT;";
    String inputErrorMessage =
        "The following errors occurred during validation: Input topic INPUT*_, name is not "
            + "correctly specified (It can contain only letters, numbers and hyphens, underscores and dots and has "
            + "to start with number or letter)";
    String outputErrorMessage =
        "The following errors occurred during validation: Output topic OUTPUT*_, name is not "
            + "correctly specified (It can contain only letters, numbers and hyphens, underscores and dots and has "
            + "to start with number or letter)";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(illegalInputKcql, inputErrorMessage);

    mapInputToOutputAssertingExceptionWithSpecificMessage(illegalOutputKcql, outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForOneToManyMappings() {
    //given
    String oneInputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT1;";
    String sameInputKcql =
        "INSERT INTO OUTPUT2 SELECT * FROM INPUT1;";
    String outputErrorMessage = "Input 'INPUT1' cannot be mapped twice.";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(oneInputKcql + sameInputKcql,
        outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForManyToOneMappings() {
    //given
    String oneInputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT2;";
    String sameInputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT1;";
    String outputErrorMessage = "Output 'OUTPUT1' cannot be mapped twice.";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(oneInputKcql + sameInputKcql,
        outputErrorMessage);
  }

  private static void mapInputToOutputAssertingExceptionWithSpecificMessage(String illegalKcql,
      String expectedMessage) {
    ConnectorStartupException configException =
        EitherValues.getLeft(KcqlConfigTopicMapper.mapInputToOutputsFromConfig(illegalKcql));
    assertThat(configException.getMessage()).contains(expectedMessage);
  }

}

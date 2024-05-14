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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class KcqlConfigBusMapperTest {

  private static final String ASTERISK = "'";
  private static final String EQUALS_SIGN = "=";
  private static final String COMMA = ",";
  private static final String PROPERTIES_BEGINNING = "PROPERTIES (";
  private static final String PROPERTIES_ENDING = ")";
  private static final String SUBSCRIPTION_NAME = "SUBSCRIPTION1";
  private static final String TOPIC_TYPE = "TOPIC";

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
      fullKcql.append(String.format(kcqlTemplate, newOutput, newInput));
      fullKcql.append(" " + createNecessaryPropertiesPart() + ";");
    }
    //when
    Map<String, String> inputToOutputsFromConfig =
        KcqlConfigBusMapper.mapInputToOutputsFromConfig(
            fullKcql.toString());

    //then
    for (String input : inputToOutputsFromConfig.keySet()) {
      int indexOfInput = inputs.indexOf(input);
      assertNotEquals(-1, indexOfInput);
      assertEquals(inputs.get(indexOfInput), input);
      assertEquals(outputs.get(indexOfInput), inputToOutputsFromConfig.get(input));
    }
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForIllegalNames() {
    //given
    String illegalInputKcql =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT*_' "
            + createNecessaryPropertiesPart() + ";";
    String illegalOutputKcql =
        "INSERT INTO 'OUTPUT*_' SELECT * FROM INPUT "
            + createNecessaryPropertiesPart() + ";";
    String inputErrorMessage =
        "Input topic INPUT*_, name is not correctly specified: "
            + "It can contain only letters, numbers and hyphens, underscores and "
            + "dots and has to start with number or letter with max size of 160";
    String outputErrorMessage =
        "Output topic OUTPUT*_, name is not correctly specified: "
            + "It can contain only letters, numbers and hyphens, underscores and "
            + "dots and has to start with number or letter with max size of 160";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(illegalInputKcql, inputErrorMessage);

    mapInputToOutputAssertingExceptionWithSpecificMessage(illegalOutputKcql, outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowWithoutNecessaryProperties() {
    //given
    String kcqlWithoutBusTypeParameter =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT';";
    String kcqlWithBadBusTypeParameter =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT' "
            + "PROPERTIES ('servicebus.type'='WRONG');";
    String kcqlOfTypeTopicWithoutSubscription =
        "INSERT INTO 'OUTPUT' SELECT * FROM INPUT "
            + "PROPERTIES ('servicebus.type'='TOPIC');";
    String typeMissingMessage =
        "Following non-optional properties missing in KCQL: servicebus.type";
    String badTypeMessage =
        "Property servicebus.type contains invalid value. Valid values are: TOPIC or QUEUE";
    String subscriptionMissingMessage =
        "Subscription name null is not correctly specified but is necessary for TOPIC: "
            + "It can contain only letters, numbers and hyphens, underscores and dots "
            + "and has to start with number or letter with max size of 50";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(kcqlWithoutBusTypeParameter, typeMissingMessage);
    mapInputToOutputAssertingExceptionWithSpecificMessage(kcqlWithBadBusTypeParameter, badTypeMessage);

    mapInputToOutputAssertingExceptionWithSpecificMessage(kcqlOfTypeTopicWithoutSubscription,
        subscriptionMissingMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForInvalidPropertyValues() {
    //given
    String kcqlWithoutCorrectType =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT' "
            + "PROPERTIES ('servicebus.type'='something','subscription.name'='sub1');";
    String kcqlWithoutCorrectSubscriptionName =
        "INSERT INTO OUTPUT SELECT * FROM 'INPUT' "
            + "PROPERTIES ('servicebus.type'='TOPIC','subscription.name'='SUB*_');";
    String busTypeError =
        "Property servicebus.type contains invalid value. Valid values are: TOPIC or QUEUE";
    String subscriptionNameError =
        "Subscription name SUB*_ is not correctly specified but is necessary for TOPIC: It can contain only letters, "
            + "numbers and hyphens, underscores and dots and has to start with number or letter with max size of 50";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(kcqlWithoutCorrectType, busTypeError);

    mapInputToOutputAssertingExceptionWithSpecificMessage(kcqlWithoutCorrectSubscriptionName, subscriptionNameError);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForOneToManyMappings() {
    //given
    String oneInputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT1 "
            + createNecessaryPropertiesPart() + ";";
    String sameInputKcql =
        "INSERT INTO OUTPUT2 SELECT * FROM INPUT1 "
            + createNecessaryPropertiesPart() + ";";
    String outputErrorMessage = "Input INPUT1 cannot be mapped twice.";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(oneInputKcql + sameInputKcql,
        outputErrorMessage);
  }

  @Test
  void mapInputToOutputsFromConfigShouldntAllowForMultipleInputsToSameOutput() {
    //given
    String oneInputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT1 "
            + createNecessaryPropertiesPart() + ";";
    String anotherInputToSameOutputKcql =
        "INSERT INTO OUTPUT1 SELECT * FROM INPUT2 "
            + createNecessaryPropertiesPart() + ";";
    String outputErrorMessage = "Output OUTPUT1 cannot be mapped twice.";

    //when
    mapInputToOutputAssertingExceptionWithSpecificMessage(
        oneInputKcql + anotherInputToSameOutputKcql,
        outputErrorMessage);
  }

  private String createPropertiesPart(Map<ServiceBusKcqlProperties, String> propertiesToAddWithValues) {
    StringBuilder stringBuilder = new StringBuilder(PROPERTIES_BEGINNING);
    for (ServiceBusKcqlProperties property : propertiesToAddWithValues.keySet()) {
      stringBuilder.append(ASTERISK).append(property.getPropertyName()).append(ASTERISK).append(EQUALS_SIGN)
          .append(ASTERISK).append(propertiesToAddWithValues.get(property)).append(ASTERISK).append(COMMA);
    }
    stringBuilder.deleteCharAt(stringBuilder.lastIndexOf(COMMA));
    stringBuilder.append(PROPERTIES_ENDING);

    return stringBuilder.toString();
  }

  private String createNecessaryPropertiesPart() {
    Map<ServiceBusKcqlProperties, String> necessaryPropertiesWithValues =
        Map.of(
            ServiceBusKcqlProperties.SERVICE_BUS_TYPE, TOPIC_TYPE,
            ServiceBusKcqlProperties.SUBSCRIPTION_NAME, SUBSCRIPTION_NAME
        );

    return createPropertiesPart(necessaryPropertiesWithValues);
  }

  private static void mapInputToOutputAssertingExceptionWithSpecificMessage(String illegalKcql,
      String expectedMessage) {
    ConfigException configException =
        assertThrows(ConfigException.class,
            () -> KcqlConfigBusMapper.mapInputToOutputsFromConfig(illegalKcql));
    assertEquals(expectedMessage, configException.getMessage());
  }
}

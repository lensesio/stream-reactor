package io.lenses.streamreactor.connect.azure.eventhubs.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
}
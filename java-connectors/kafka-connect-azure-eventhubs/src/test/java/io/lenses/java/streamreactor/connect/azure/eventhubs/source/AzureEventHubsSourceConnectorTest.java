package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static org.apache.kafka.connect.source.ExactlyOnceSupport.SUPPORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.Test;

class AzureEventHubsSourceConnectorTest {

  private static final String CONNECTOR_NAME = "AzureEventHubsSource";
  private AzureEventHubsSourceConnector testObj = new AzureEventHubsSourceConnector();

  @Test
  void taskConfigsShouldSplitKcqlConfigs() throws NoSuchFieldException, IllegalAccessException {
    //given
    Map<String, String> simpleProperties = createSimplePropertiesWithoutKcql();
    int numberOfKcqlStatements = 3;
    Map<String, String> inputToOutput = new HashMap<>(numberOfKcqlStatements);
    StringBuilder joinedKcqlQueryBuilder = new StringBuilder();
    String[] singleKcqlQueries = new String[numberOfKcqlStatements];

    for (int i = 0; i < numberOfKcqlStatements; i++){
      String input = String.format("INPUT%s", i);
      String output = String.format("OUTPUT%s", i);
      inputToOutput.put(input, output);
      String query = String.format("INSERT INTO %s SELECT * FROM %s;", input, output);
      singleKcqlQueries[i] = query;
      joinedKcqlQueryBuilder.append(query);
    }

    simpleProperties.put(AzureEventHubsConfigConstants.KCQL_CONFIG, joinedKcqlQueryBuilder.toString());

    //when

    Field pinCodeField = testObj.getClass()
        .getDeclaredField("configProperties");
    pinCodeField.setAccessible(true);
    pinCodeField.set(testObj, simpleProperties);
    List<Map<String, String>> taskConfigs = testObj.taskConfigs(numberOfKcqlStatements);

    //then
    assertEquals(3, taskConfigs.size());
    for (int i = 0; i < numberOfKcqlStatements; i++){
      Map<String, String> taskConfig = taskConfigs.get(i);
      assertEquals(singleKcqlQueries[i], taskConfig.get(AzureEventHubsConfigConstants.KCQL_CONFIG));
    }

    }

  @Test
  void exactlyOnceSupportShouldReturnSupported() {
    //given

    //when
    ExactlyOnceSupport exactlyOnceSupport = testObj.exactlyOnceSupport(new HashMap<>());

    //then
    SUPPORTED.equals(exactlyOnceSupport);
  }

  private Map<String, String> createSimplePropertiesWithoutKcql(){
    Map<String, String> properties = new HashMap<>();

    properties.put(AzureEventHubsConfigConstants.CONNECTOR_NAME, CONNECTOR_NAME);
    properties.put("connector.class", AzureEventHubsSourceConnector.class.getCanonicalName());

    return properties;
  }
}
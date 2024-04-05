package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.apache.kafka.connect.source.ExactlyOnceSupport.SUPPORTED;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.eventhubs.config.AzureEventHubsConfigConstants;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class AzureEventHubsSourceConnectorTest {

  private static final String CONNECTOR_NAME = "AzureEventHubsSource";
  private static final String KCQL = "INSERT INTO OUTPUT1 SELECT * FROM INPUT1;";
  private AzureEventHubsSourceConnector testObj;

  @BeforeEach
  void setUp() {
    try (MockedConstruction<JarManifest> ignored = Mockito.mockConstruction(JarManifest.class)) {
      testObj = new AzureEventHubsSourceConnector();
    }
  }

  @Test
  void taskConfigsShouldMultiplyConfigs() throws NoSuchFieldException, IllegalAccessException {
    //given
    Map<String, String> simpleProperties = createSimplePropertiesWithKcql();
    int maxTasks = 3;

    //when
    Field configPropertiesField = testObj.getClass()
        .getDeclaredField("configProperties");
    configPropertiesField.setAccessible(true);
    configPropertiesField.set(testObj, simpleProperties);
    List<Map<String, String>> taskConfigs = testObj.taskConfigs(maxTasks);

    //then
    for (Map<String, String> taskConfig : taskConfigs){
      assertTrue(taskConfig.equals(simpleProperties));
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

  private Map<String, String> createSimplePropertiesWithKcql() {
    Map<String, String> properties = new HashMap<>();

    properties.put(AzureEventHubsConfigConstants.CONNECTOR_NAME, CONNECTOR_NAME);
    properties.put("connector.class", AzureEventHubsSourceConnector.class.getCanonicalName());
    properties.put(AzureEventHubsConfigConstants.KCQL_CONFIG, KCQL);

    return properties;
  }
}
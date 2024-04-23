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
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.apache.kafka.connect.source.ExactlyOnceSupport.SUPPORTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    assertEquals(SUPPORTED, exactlyOnceSupport);
  }

  private Map<String, String> createSimplePropertiesWithKcql() {
    Map<String, String> properties = Map.of(
        AzureEventHubsConfigConstants.CONNECTOR_NAME, CONNECTOR_NAME,
        "connector.class", AzureEventHubsSourceConnector.class.getCanonicalName(),
        AzureEventHubsConfigConstants.KCQL_CONFIG, KCQL
    );

    return properties;
  }
}
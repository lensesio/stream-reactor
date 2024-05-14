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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.common.util.JarManifest;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusConfigConstants;
import io.lenses.streamreactor.connect.azure.servicebus.config.AzureServiceBusSourceConfig;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

class AzureServiceBusSourceConnectorTest {

  private static final String CONFIG_CONNECTOR_NAME = "AzureServiceBusSourceConnector";
  private static final String EMPTY_STRING = "";
  private static final String JARMANIFEST_VERSION = "1.2.3";
  private static final String JARMANIFEST_STRING = "some JarManifest String";
  private AzureServiceBusSourceConnector testObj;
  private JarManifest jarManifest;

  @BeforeEach
  void setUpAll() {
    try (MockedConstruction<JarManifest> ignored = Mockito.mockConstruction(JarManifest.class)) {
      testObj = new AzureServiceBusSourceConnector();
      jarManifest = ignored.constructed().get(0);
    }
  }

  @Test
  void start() {
    //given
    when(jarManifest.buildManifestString()).thenReturn(JARMANIFEST_STRING);
    Map<String, String> validProperties = buildValidProperties();

    //when
    testObj.start(validProperties);

    //then
    verify(jarManifest).buildManifestString();
  }

  @Test
  void taskClassShouldReturnTaskClass() {
    //when
    Class<? extends Task> taskClass = testObj.taskClass();

    //then
    AzureServiceBusSourceTask.class.equals(taskClass);
  }

  @Test
  void taskConfigsShouldReturnSameConfigForSimplePropertiesWithOneKcqlAndTwoMaxTasks() {
    //given
    Map<String, String> validProperties = buildValidProperties();
    int maxTasksForThisScenario = 2;

    //when
    testObj.start(validProperties);
    List<Map<String, String>> taskConfigs = testObj.taskConfigs(maxTasksForThisScenario);

    //then
    assertThat(taskConfigs).hasSize(2);
    assertThat(taskConfigs.get(0)).isEqualTo(validProperties);
    assertThat(taskConfigs.get(1)).isEqualTo(validProperties);
  }

  @Test
  void configShouldReturnConfigClass() {
    //when
    ConfigDef configDef = testObj.config();

    //then
    AzureServiceBusSourceConfig.getConfigDefinition().equals(configDef);
  }

  @Test
  void versionShouldReturnJarManifestsVersion() {
    //given
    when(jarManifest.getVersion()).thenReturn(JARMANIFEST_VERSION);

    //when
    String version = testObj.version();

    //then
    assertThat(version).isEqualTo(JARMANIFEST_VERSION);
  }

  private Map<String, String> buildValidProperties() {
    String simpleKcql = buildKcqlWithNecessaryProperties();
    HashMap<String, String> propertyMap = new HashMap<>();

    propertyMap.put(AzureServiceBusConfigConstants.CONNECTOR_NAME, CONFIG_CONNECTOR_NAME);
    propertyMap.put(AzureServiceBusConfigConstants.CONNECTION_STRING, EMPTY_STRING);
    propertyMap.put(AzureServiceBusConfigConstants.KCQL_CONFIG, simpleKcql);

    return propertyMap;
  }

  private String buildKcqlWithNecessaryProperties() {
    final StringBuilder kcqlBuilder = new StringBuilder("INSERT INTO output SELECT * FROM input ");
    kcqlBuilder.append("PROPERTIES(");

    for (ServiceBusKcqlProperties property : ServiceBusKcqlProperties.values()) {
      kcqlBuilder.append("'" + property.getPropertyName() + "'='" + property.getPropertyName() + "',");
    }

    //delete comma after last property, close properties
    kcqlBuilder.deleteCharAt(kcqlBuilder.lastIndexOf(",")).append(")");

    return kcqlBuilder.toString();
  }
}

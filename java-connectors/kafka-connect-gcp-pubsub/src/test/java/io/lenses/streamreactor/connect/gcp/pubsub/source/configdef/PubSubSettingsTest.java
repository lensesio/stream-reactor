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
package io.lenses.streamreactor.connect.gcp.pubsub.source.configdef;

import static io.lenses.streamreactor.test.utils.EitherValues.assertRight;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.MapConfigSource;
import lombok.val;

class PubSubSettingsTest {

  private static final ConnectorPrefix connectorPrefix = new ConnectorPrefix("pubsub.test");
  public static final String TEST_PROJECT_ID = "test-project-id";

  @Test
  void shouldInjectGcpSettingsIntoConfigDef() {
    val configDef = new ConfigDef();
    new PubSubSettings(connectorPrefix).withSettings(configDef);

    val projectIdConfigSetting = configDef.configKeys().get("pubsub.test.gcp.project.id");
    assertEquals("", projectIdConfigSetting.defaultValue);
    assertEquals(ConfigDef.Type.STRING, projectIdConfigSetting.type);
    assertEquals(ConfigDef.Importance.HIGH, projectIdConfigSetting.importance);
    assertEquals("GCP Project ID", projectIdConfigSetting.documentation);
  }

  @Test
  void shouldParsePubSubConfigFromConfigSource() {
    val configSource =
        new MapConfigSource(
            Map.of(
                "pubsub.test.gcp.project.id", TEST_PROJECT_ID

            )
        );

    val pubSubConfig = new PubSubSettings(connectorPrefix).parseFromConfig(configSource);

    assertRight(pubSubConfig).hasFieldOrPropertyWithValue("projectId", TEST_PROJECT_ID);
  }

}

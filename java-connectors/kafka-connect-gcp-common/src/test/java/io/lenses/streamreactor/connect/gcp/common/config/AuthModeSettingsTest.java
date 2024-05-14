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
package io.lenses.streamreactor.connect.gcp.common.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.common.config.source.MapConfigSource;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.CredentialsAuthMode;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.DefaultAuthMode;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.FileAuthMode;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.NoAuthMode;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AuthModeSettingsTest {

  private AuthModeSettings authModeSettings;
  private final String CONNECTOR_PREFIX = "test.connector";

  @BeforeEach
  public void setUp() {
    final var connectorPrefix = new ConnectorPrefix(CONNECTOR_PREFIX);
    authModeSettings = new AuthModeSettings(connectorPrefix);
  }

  @Test
  void testGenerateKey() {
    assertEquals("test.connector.gcp.auth.mode", authModeSettings.getAuthModeKey());
    assertEquals("test.connector.gcp.credentials", authModeSettings.getCredentialsKey());
    assertEquals("test.connector.gcp.file", authModeSettings.getFileKey());
  }

  @Test
  void testWithAuthModeSettings() {
    final var configDef = new ConfigDef();
    final var result = authModeSettings.withSettings(configDef);

    assertNotNull(result);
    assertTrue(result.configKeys().containsKey(authModeSettings.getAuthModeKey()));
    assertTrue(result.configKeys().containsKey(authModeSettings.getCredentialsKey()));
    assertTrue(result.configKeys().containsKey(authModeSettings.getFileKey()));
  }

  @Test
  void testParseFromConfig_CredentialsAuthMode() {
    final var configMap =
        new MapConfigSource(
            Map.of(
                authModeSettings.getAuthModeKey(),
                "credentials",
                authModeSettings.getCredentialsKey(),
                new Password("password")));

    final var authMode = authModeSettings.parseFromConfig(configMap);

    assertNotNull(authMode);
    assertTrue(authMode instanceof CredentialsAuthMode);
  }

  @Test
  void testParseFromConfig_FileAuthMode() {
    final var configMap =
        new MapConfigSource(
            Map.of(
                authModeSettings.getAuthModeKey(), "file",
                authModeSettings.getFileKey(), "\"path/to/file\""));
    final var authMode = authModeSettings.parseFromConfig(configMap);

    assertNotNull(authMode);
    assertTrue(authMode instanceof FileAuthMode);
  }

  @Test
  void testParseFromConfig_NoneAuthMode() {

    final var configMap = new MapConfigSource(Map.of(authModeSettings.getAuthModeKey(), "none"));
    final var authMode = authModeSettings.parseFromConfig(configMap);

    assertNotNull(authMode);
    assertTrue(authMode instanceof NoAuthMode);
  }

  @Test
  void testParseFromConfig_DefaultAuthMode() {

    final var configMap = new MapConfigSource(Map.of(authModeSettings.getAuthModeKey(), "default"));

    final var authMode = authModeSettings.parseFromConfig(configMap);

    assertNotNull(authMode);
    assertTrue(authMode instanceof DefaultAuthMode);
  }

  @Test
  void testParseFromConfig_UnsupportedAuthMode() {

    final var configMap = new MapConfigSource(Map.of(authModeSettings.getAuthModeKey(), "invalid"));

    assertThrows(ConfigException.class, () -> authModeSettings.parseFromConfig(configMap));
  }
}

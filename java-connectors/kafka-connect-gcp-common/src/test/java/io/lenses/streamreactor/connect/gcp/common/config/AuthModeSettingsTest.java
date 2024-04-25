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
package io.lenses.streamreactor.connect.gcp.common.config;
import io.lenses.streamreactor.common.config.base.ConfigMap;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.*;
import lombok.val;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AuthModeSettingsTest {

    private AuthModeSettings authModeSettings;
    private final String CONNECTOR_PREFIX = "test.connector";

    @BeforeEach
    public void setUp() {
        authModeSettings = new AuthModeSettings(CONNECTOR_PREFIX);
    }

    @Test
    void testGenerateKey() {
        assertEquals("test.connector.gcp.auth.mode", authModeSettings.getAuthModeKey());
        assertEquals("test.connector.gcp.credentials", authModeSettings.getCredentialsKey());
        assertEquals("test.connector.gcp.file", authModeSettings.getFileKey());
    }

    @Test
    void testWithAuthModeSettings() {
        val configDef = new ConfigDef();
        val result = authModeSettings.withAuthModeSettings(configDef);

        assertNotNull(result);
        assertTrue(result.configKeys().containsKey(authModeSettings.getAuthModeKey()));
        assertTrue(result.configKeys().containsKey(authModeSettings.getCredentialsKey()));
        assertTrue(result.configKeys().containsKey(authModeSettings.getFileKey()));
    }

    @Test
    void testParseFromConfig_CredentialsAuthMode() {
        val configMap = new ConfigMap(
                Map.of(
                        authModeSettings.getAuthModeKey(), "credentials",
                        authModeSettings.getCredentialsKey(), new Password("password")
                )
        );

        val authMode = authModeSettings.parseFromConfig(configMap);

        assertNotNull(authMode);
        assertTrue(authMode instanceof CredentialsAuthMode);
    }

    @Test
    void testParseFromConfig_FileAuthMode() {

        val configMap = new ConfigMap(
                Map.of(
                        authModeSettings.getAuthModeKey(), "file",
                        authModeSettings.getFileKey(), "\"path/to/file\""
                )
        );
        val authMode = authModeSettings.parseFromConfig(configMap);

        assertNotNull(authMode);
        assertTrue(authMode instanceof FileAuthMode);
    }

    @Test
    void testParseFromConfig_NoneAuthMode() {

        val configMap = new ConfigMap(
                Map.of(
                        authModeSettings.getAuthModeKey(), "none"
                )
        );
        val authMode = authModeSettings.parseFromConfig(configMap);

        assertNotNull(authMode);
        assertTrue(authMode instanceof NoAuthMode);
    }

    @Test
    void testParseFromConfig_DefaultAuthMode() {

        val configMap = new ConfigMap(
                Map.of(
                        authModeSettings.getAuthModeKey(), "default"
                )
        );

        val authMode = authModeSettings.parseFromConfig(configMap);

        assertNotNull(authMode);
        assertTrue(authMode instanceof DefaultAuthMode);
    }

    @Test
    void testParseFromConfig_UnsupportedAuthMode() {

        val configMap = new ConfigMap(
                Map.of(
                        authModeSettings.getAuthModeKey(), "invalid"
                )
        );

        assertThrows(ConfigException.class, () -> authModeSettings.parseFromConfig(configMap));
    }
}

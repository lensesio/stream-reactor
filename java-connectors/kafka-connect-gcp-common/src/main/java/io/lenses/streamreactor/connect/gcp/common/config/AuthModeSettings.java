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
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix;
import io.lenses.streamreactor.connect.gcp.common.auth.mode.*;
import lombok.Getter;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

/**
 * Configuration settings for specifying authentication mode and related credentials for GCP connectors.
 * This class provides methods to define and parse authentication mode settings based on Kafka Connect's {@link ConfigDef}.
 *
 * Authentication modes supported:
 * - 'credentials': Use GCP credentials for authentication.
 * - 'file': Authenticate using credentials stored in a file.
 * - 'default': Default authentication mode.
 * - 'none': No authentication required.
 *
 * Keys used in configuration:
 * - {@code gcp.auth.mode}: Key to specify the authentication mode.
 * - {@code gcp.credentials}: Key for providing GCP credentials (used with 'credentials' auth mode).
 * - {@code gcp.file}: Key for specifying the file path containing GCP credentials (used with 'file' auth mode).
 */
@Getter
public class AuthModeSettings {

    public static final String EMPTY_STRING = "";

    // Auth Mode values
    public static final String PROP_KEY_CREDENTIALS = "CREDENTIALS";
    public static final String PROP_KEY_FILE = "FILE";
    public static final String PROP_KEY_NONE = "NONE";
    public static final String PROP_KEY_DEFAULT = "DEFAULT";

    private final String authModeKey;
    private final String credentialsKey;
    private final String fileKey;

    /**
     * Constructs an instance of AuthModeSettings.
     *
     * @param connectorPrefix The prefix used to generate keys for configuration settings.
     */
    public AuthModeSettings(ConnectorPrefix connectorPrefix) {
        authModeKey = connectorPrefix.prefixKey("gcp.auth.mode");
        credentialsKey = connectorPrefix.prefixKey("gcp.credentials");
        fileKey = connectorPrefix.prefixKey("gcp.file");
    }


    /**
     * Configures the provided ConfigDef with authentication mode settings.
     *
     * @param configDef The ConfigDef instance to be updated with authentication mode definitions.
     * @return The updated ConfigDef with authentication mode settings defined.
     */
    public ConfigDef withAuthModeSettings(ConfigDef configDef) {
        return configDef.define(
                        authModeKey,
                        Type.STRING,
                        PROP_KEY_DEFAULT,
                        Importance.HIGH,
                        "Authenticate mode, 'credentials', 'file', 'default' or 'none'"
                        )
                .define(
                        credentialsKey,
                        Type.PASSWORD,
                        EMPTY_STRING,
                        Importance.HIGH,
                        "GCP Credentials if using 'credentials' auth mode."
                        )
                .define(
                        fileKey,
                        Type.STRING,
                        EMPTY_STRING,
                        Importance.HIGH,
                        "File containing GCP Credentials if using 'file' auth mode.  This can be relative from the current working directory of the java process or from the root.  Remember your path format is operating system dependent. (eg for unix-based /home/my/path/file)"
                        );
    }

    /**
     * Parses authentication mode from the provided ConfigMap and returns the corresponding AuthMode instance.
     *
     * @param configMap The ConfigMap containing configuration settings.
     * @return The parsed AuthMode based on the configuration settings.
     * @throws ConfigException If an invalid or unsupported authentication mode is specified.
     */
    public AuthMode parseFromConfig(ConfigMap configMap) {
        return configMap.getString(getAuthModeKey())
                .map(authModeString -> {
                    switch (authModeString.toUpperCase()) {
                        case PROP_KEY_CREDENTIALS:
                            return createCredentialsAuthMode(configMap);
                        case PROP_KEY_FILE:
                            return createFileAuthMode(configMap);
                        case PROP_KEY_NONE:
                            return new NoAuthMode();
                        case PROP_KEY_DEFAULT:
                            return new DefaultAuthMode();
                        case EMPTY_STRING:
                        default:
                            throw new ConfigException(String.format("Unsupported auth mode `%s`", authModeString));
                    }
                })
                .orElse(new DefaultAuthMode());
    }

    private FileAuthMode createFileAuthMode(ConfigMap configMap) {
        return configMap.getString(getFileKey()).map(FileAuthMode::new).orElseThrow(() -> new ConfigException(String.format("No `%s` specified in configuration", getFileKey())));
    }

    private CredentialsAuthMode createCredentialsAuthMode(ConfigMap configMap) {
        return configMap.getPassword(getCredentialsKey()).map(CredentialsAuthMode::new).orElseThrow(() -> new ConfigException(String.format("No `%s` specified in configuration", getCredentialsKey())));
    }
}
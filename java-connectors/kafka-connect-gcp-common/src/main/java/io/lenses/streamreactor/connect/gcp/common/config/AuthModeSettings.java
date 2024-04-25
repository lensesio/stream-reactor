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
import lombok.AllArgsConstructor;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

@AllArgsConstructor
public class AuthModeSettings {

    private final String connectorPrefix;

    public String getAuthModeKey() {
        return generateKey("gcp.auth.mode");
    }

    public String getCredentialsKey() {
        return generateKey("gcp.credentials");
    }

    public String getFileKey() {
        return generateKey("gcp.file");
    }

    private String generateKey(String suffix) {
        return String.format("%s.%s", connectorPrefix, suffix);
    }


    public ConfigDef withAuthModeSettings(ConfigDef configDef) {
        return configDef.define(
                        getAuthModeKey(),
                        Type.STRING,
                        AuthModeEnum.DEFAULT.name(),
                        Importance.HIGH,
                        "Authenticate mode, 'credentials', 'file' or 'default'"
                        )
                .define(
                        getCredentialsKey(),
                        Type.PASSWORD,
                        "",
                        Importance.HIGH,
                        "GCP Credentials if using 'credentials' auth mode."
                        )
                .define(
                        getFileKey(),
                        Type.STRING,
                        "",
                        Importance.HIGH,
                        "File containing GCP Credentials if using 'file' auth mode"
                        );
    }

    public AuthMode parseFromConfig(ConfigMap configMap) {
        return configMap.getString(getAuthModeKey())
                .map(authModeStr -> AuthModeEnum.valueOfCaseInsensitiveOptional(authModeStr)
                        .orElseThrow(() -> new ConfigException(String.format("Unsupported auth mode `%s`", authModeStr))))
                .map(authModeEnum -> {
                    switch (authModeEnum) {
                        case CREDENTIALS:
                            return createCredentialsAuthMode(configMap);
                        case FILE:
                            return createFileAuthMode(configMap);
                        case NONE:
                            return NoAuthMode.INSTANCE;
                        case DEFAULT:
                        default:
                            return DefaultAuthMode.INSTANCE;
                    }
                })
                .orElse(DefaultAuthMode.INSTANCE);
    }

    private FileAuthMode createFileAuthMode(ConfigMap configMap) {
        return configMap.getString(getFileKey()).map(FileAuthMode::new).orElseThrow(() -> new ConfigException(String.format("No `%s` specified in configuration", getFileKey())));
    }

    private CredentialsAuthMode createCredentialsAuthMode(ConfigMap configMap) {
        return configMap.getPassword(getCredentialsKey()).map(CredentialsAuthMode::new).orElseThrow(() -> new ConfigException(String.format("No `%s` specified in configuration", getCredentialsKey())));
    }
}
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
package io.lenses.streamreactor.connect.gcp.common.auth.mode;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.kafka.common.config.types.Password;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Authentication mode using credentials from a string in configuration.
 */
@AllArgsConstructor
@EqualsAndHashCode
public class CredentialsAuthMode implements AuthMode {
    private final Password passwordCredentials;

    @Override
    public Credentials getCredentials() throws IOException {
        return GoogleCredentials.fromStream(
                new ByteArrayInputStream(passwordCredentials.value().getBytes())
        );
    }
}

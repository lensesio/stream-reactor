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

import java.io.IOException;
import java.util.Optional;

/**
 * Default authentication mode without explicit credentials.
 * This mode utilizes the Application Default Credentials (ADC) chain.
 * ADC is a strategy used by the Google authentication libraries to automatically find credentials based on the application environment.
 * The credentials are made available to Cloud Client Libraries and Google API Client Libraries, allowing the code to run seamlessly
 * in both development and production environments without altering the authentication process for Google Cloud services and APIs.
 */
public class DefaultAuthMode implements AuthMode {
    private DefaultAuthMode() {
    }

    public static AuthMode INSTANCE = new DefaultAuthMode();

    @Override
    public Credentials getCredentials() throws IOException {
        return GoogleCredentials.getApplicationDefault();
    }
}

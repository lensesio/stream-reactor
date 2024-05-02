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

import java.io.IOException;

/**
 * Interface representing different authentication modes for GCP connectors.
 * Implementations of this interface provide methods to obtain Google Cloud Platform (GCP) {@link Credentials}.
 */
public interface AuthMode {

    /**
     * Retrieves the GCP credentials required for authentication.
     *
     * @return The GCP {@link Credentials}.
     * @throws IOException If an I/O error occurs while obtaining credentials.
     */
    Credentials getCredentials() throws IOException;
}


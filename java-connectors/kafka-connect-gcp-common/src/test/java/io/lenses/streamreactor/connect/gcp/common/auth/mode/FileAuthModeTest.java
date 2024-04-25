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

import com.google.auth.oauth2.ServiceAccountCredentials;
import lombok.val;
import org.junit.jupiter.api.Test;

import static io.lenses.streamreactor.connect.gcp.common.auth.mode.TestFileUtil.absolutePathForResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FileAuthModeTest {

    @Test
    void getCredentialsShouldReturnCredentials() throws Exception {
        val filePath = absolutePathForResource("/test-gcp-credentials.json");
        val authMode = new FileAuthMode(filePath);

        val googleCredentials = (ServiceAccountCredentials) authMode.getCredentials();
        assertEquals("your-client-id", googleCredentials.getClientId());
    }

}
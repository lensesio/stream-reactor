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
package io.lenses.streamreactor.common.config.base;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigAdaptorTest {

    private ConnectConfig connectConfig;

    @BeforeEach
    void setUp() {
        AbstractConfig config = mock(AbstractConfig.class);
        when(config.getString("username")).thenReturn("user123");
        when(config.getPassword("password")).thenReturn(new Password("secret"));

        connectConfig = new ConfigAdaptor(config);
    }

    @Test
    void testGetString_existingKey_shouldReturnValue() {
        Optional<String> value = connectConfig.getString("username");

        assertTrue(value.isPresent());
        assertEquals("user123", value.get());
    }

    @Test
    void testGetString_nonExistingKey_shouldReturnEmpty() {
        Optional<String> value = connectConfig.getString("invalidKey");

        assertFalse(value.isPresent());
    }

    @Test
    void testGetPassword_existingKey_shouldReturnPassword() {
        Optional<Password> password = connectConfig.getPassword("password");

        assertTrue(password.isPresent());
        assertEquals("secret", password.get().value());
    }

    @Test
    void testGetPassword_nonExistingKey_shouldReturnEmpty() {
        Optional<Password> password = connectConfig.getPassword("invalidKey");

        assertFalse(password.isPresent());
    }
}

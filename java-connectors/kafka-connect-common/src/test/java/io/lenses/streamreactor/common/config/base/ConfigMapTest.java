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

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigMapTest {

    private ConfigMap configMap;

    @BeforeEach
    void setUp() {
        Map<String, Object> testMap = new HashMap<>();
        testMap.put("username", "user123");
        testMap.put("password", new Password("secret"));

        configMap = new ConfigMap(testMap);
    }

    @Test
    void testGetString_existingKey_shouldReturnValue() {
        Optional<String> value = configMap.getString("username");

        assertTrue(value.isPresent());
        assertEquals("user123", value.get());
    }

    @Test
    void testGetString_nonExistingKey_shouldReturnEmpty() {
        Optional<String> value = configMap.getString("invalidKey");

        assertFalse(value.isPresent());
    }

    @Test
    void testGetPassword_existingKey_shouldReturnPassword() {
        Optional<Password> password = configMap.getPassword("password");

        assertTrue(password.isPresent());
        assertEquals("secret", password.get().value());
    }

    @Test
    void testGetPassword_nonExistingKey_shouldReturnEmpty() {
        Optional<Password> password = configMap.getPassword("invalidKey");

        assertFalse(password.isPresent());
    }
}

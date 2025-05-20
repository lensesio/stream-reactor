/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.common.config.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

abstract class ConfigSourceTestBase {

  protected static final String PASSWORD_KEY = "password";
  protected static final Password PASSWORD_VALUE = new Password("secret");
  protected static final String BOOLEAN_KEY = "boolean";
  protected static final Boolean BOOLEAN_VALUE = true;
  protected static final String USERNAME_KEY = "username";
  protected static final String USERNAME_VALUE = "user123";
  protected static final String NULL_KEY = "valueNull";
  protected static final String NULL_VALUE = null;
  private ConfigSource configSource;

  @BeforeEach
  public void setUp() {
    configSource = createConfigSource();
  }

  abstract ConfigSource createConfigSource();

  @Test
  void testGetString_existingKey_shouldReturnValue() {
    Optional<String> value = configSource.getString(USERNAME_KEY);

    assertTrue(value.isPresent());
    assertEquals(USERNAME_VALUE, value.get());
  }

  @Test
  void testGetString_nonExistingKey_shouldReturnEmpty() {
    Optional<String> value = configSource.getString("invalidKey");

    assertFalse(value.isPresent());
  }

  @Test
  void testGetPassword_existingKey_shouldReturnPassword() {
    Optional<Password> password = configSource.getPassword(PASSWORD_KEY);

    assertTrue(password.isPresent());
    assertEquals(PASSWORD_VALUE, password.get());
  }

  @Test
  void testGetBoolean_existingKey_shouldReturnBoolean() {
    Optional<Boolean> booleanValue = configSource.getBoolean(BOOLEAN_KEY);

    assertTrue(booleanValue.isPresent());
    assertEquals(BOOLEAN_VALUE, booleanValue.get());
  }

  @Test
  void testGetPassword_nonExistingKey_shouldReturnEmpty() {
    Optional<Password> password = configSource.getPassword("invalidKey");

    assertFalse(password.isPresent());
  }

  @Test
  void testGetString_nonExistingValue_shouldReturnEmpty() {
    Optional<String> password = configSource.getString(NULL_KEY);

    assertFalse(password.isPresent());
  }
}

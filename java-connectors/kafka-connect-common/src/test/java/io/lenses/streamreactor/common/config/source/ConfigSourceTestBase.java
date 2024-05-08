/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

abstract class ConfigSourceTestBase {

  protected static final String PASSWORD_KEY = "password";
  protected static final Password PASSWORD_VALUE = new Password("secret");
  protected static final String USERNAME_KEY = "username";
  protected static final String USERNAME_VALUE = "user123";
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
  void testGetPassword_nonExistingKey_shouldReturnEmpty() {
    Optional<Password> password = configSource.getPassword("invalidKey");

    assertFalse(password.isPresent());
  }
}

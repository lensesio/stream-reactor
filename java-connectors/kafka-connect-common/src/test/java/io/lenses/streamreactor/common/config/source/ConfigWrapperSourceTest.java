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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.kafka.common.config.AbstractConfig;

class ConfigWrapperSourceTest extends ConfigSourceTestBase {

  ConfigSource createConfigSource() {

    AbstractConfig config = mock(AbstractConfig.class);
    when(config.getString(USERNAME_KEY)).thenReturn(USERNAME_VALUE);
    when(config.getPassword(PASSWORD_KEY)).thenReturn(PASSWORD_VALUE);
    when(config.getString(NULL_KEY)).thenReturn(NULL_VALUE);

    return new ConfigWrapperSource(config);
  }
}

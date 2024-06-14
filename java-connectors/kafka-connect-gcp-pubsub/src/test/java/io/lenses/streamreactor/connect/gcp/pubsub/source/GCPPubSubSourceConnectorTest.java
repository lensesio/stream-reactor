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
package io.lenses.streamreactor.connect.gcp.pubsub.source;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSourceConfig;
import lombok.val;

class GCPPubSubSourceConnectorTest {

  private PubSubSourceConfig pubSubSourceConfig;

  private GCPPubSubSourceConnector target;

  @BeforeEach
  void setUp() {
    target = new GCPPubSubSourceConnector();
  }

  @Test
  void startWithValidPropsDoesNotThrowException() {

    val props = Map.of("connect.pubsub.kcql", "insert into blah select * from blee");

    target.start(props);

  }

  @Test
  void startWithInvalidPropsThrowsConfigException() {

    val props = Map.of("", "");

    assertThatThrownBy(() -> target.start(props))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Missing required configuration \"connect.pubsub.kcql\" which has no default value.");

  }
}

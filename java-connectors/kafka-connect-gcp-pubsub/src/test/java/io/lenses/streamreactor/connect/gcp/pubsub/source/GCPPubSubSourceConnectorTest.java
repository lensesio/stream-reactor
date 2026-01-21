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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.val;

class GCPPubSubSourceConnectorTest {

  private static final String KCQL_KEY = "connect.pubsub.kcql";

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

  @Test
  void taskConfigsReplicatesPropsToAllTasks() {
    val kcql = "insert into kafka-topic select * from subscription";
    val props = Map.of(KCQL_KEY, kcql);

    target.start(props);
    val taskConfigs = target.taskConfigs(3);

    assertThat(taskConfigs).hasSize(3);
    for (val taskConfig : taskConfigs) {
      assertThat(taskConfig.get(KCQL_KEY)).isEqualTo(kcql);
    }
  }

  @Test
  void taskConfigsReplicatesAllKcqlStatementsToAllTasks() {
    val kcql = "insert into topic1 select * from sub1;insert into topic2 select * from sub2";
    val props = Map.of(KCQL_KEY, kcql);

    target.start(props);
    val taskConfigs = target.taskConfigs(3);

    assertThat(taskConfigs).hasSize(3);
    for (val taskConfig : taskConfigs) {
      assertThat(taskConfig.get(KCQL_KEY)).isEqualTo(kcql);
    }
  }

  @Test
  void taskConfigsWithSingleTaskReturnsOneConfig() {
    val kcql = "insert into kafka-topic select * from subscription";
    val props = Map.of(KCQL_KEY, kcql);

    target.start(props);
    val taskConfigs = target.taskConfigs(1);

    assertThat(taskConfigs).hasSize(1);
    assertThat(taskConfigs.get(0).get(KCQL_KEY)).isEqualTo(kcql);
  }

  @Test
  void taskConfigsWithZeroTasksReturnsEmptyList() {
    val kcql = "insert into kafka-topic select * from subscription";
    val props = Map.of(KCQL_KEY, kcql);

    target.start(props);
    val taskConfigs = target.taskConfigs(0);

    assertThat(taskConfigs).isEmpty();
  }
}

/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.common.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.lenses.streamreactor.common.config.base.KcqlSettings;
import lombok.val;

class TasksSplitterTest {

  private static final String KCQL_SETTINGS_KEY = "connect.some.prefix.kcql";

  private static final String OTHER_KEY = "key1";

  private static final String OTHER_VALUE = "value1";

  @ParameterizedTest
  @MethodSource("testCases")
  void testSplitTasksByKcqlStatements(String joinedKcqlStatements, int maxTasks, List<String> expectedKcqls) {
    Map<String, String> props =
        Map.of(
            OTHER_KEY, OTHER_VALUE,
            KCQL_SETTINGS_KEY, joinedKcqlStatements
        );

    val kcqlSettings = mock(KcqlSettings.class);
    when(kcqlSettings.getKcqlSettingsKey()).thenReturn(KCQL_SETTINGS_KEY);

    val result = TasksSplitter.splitByKcqlStatements(maxTasks, props, kcqlSettings);

    assertEquals(expectedKcqls.size(), result.size());
    for (int i = 0; i < expectedKcqls.size(); i++) {
      val taskProps = result.get(i);
      assertEquals(OTHER_VALUE, taskProps.get(OTHER_KEY));
      assertEquals(expectedKcqls.get(i), taskProps.get(KCQL_SETTINGS_KEY));
    }
  }

  private static Stream<Arguments> testCases() {
    return Stream.of(
        Arguments.of("INSERT INTO * SELECT * FROM topicA", 1, Collections.singletonList(
            "INSERT INTO * SELECT * FROM topicA")),
        Arguments.of("INSERT INTO * SELECT * FROM topicA;INSERT INTO * SELECT * FROM topicB", 1, Collections
            .singletonList(
                "INSERT INTO * SELECT * FROM topicA;INSERT INTO * SELECT * FROM topicB")),
        Arguments.of(
            "INSERT INTO * SELECT * FROM topicA;INSERT INTO * SELECT * FROM topicB;INSERT INTO * SELECT * FROM topicC",
            2, Arrays.asList(
                "INSERT INTO * SELECT * FROM topicA;INSERT INTO * SELECT * FROM topicB",
                "INSERT INTO * SELECT * FROM topicC")),
        Arguments.of("", 1, Collections.singletonList("")),
        Arguments.of("INSERT INTO * SELECT * FROM topicA;INSERT INTO * SELECT * FROM topicB", 3, Arrays.asList(
            "INSERT INTO * SELECT * FROM topicA",
            "INSERT INTO * SELECT * FROM topicB"))
    );
  }
}

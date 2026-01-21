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
package io.lenses.streamreactor.common.util;

import static io.lenses.kcql.Kcql.KCQL_MULTI_STATEMENT_SEPARATOR;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.lenses.streamreactor.common.config.base.KcqlSettings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.val;

/**
 * Utility class for splitting and replicating tasks based on KCQL statements.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TasksSplitter {

  /**
   * Splits tasks based on the KCQL statements provided in the properties map.
   * Each resulting map will contain the original properties and a subset of the KCQL statements.
   *
   * @param maxTasks     the maximum number of tasks to split into
   * @param props        the original properties map containing KCQL settings
   * @param kcqlSettings the KCQL settings object that provides the key for KCQL settings in the properties map
   * @return a list of maps, each containing the original properties and a subset of the KCQL statements
   */
  public static List<Map<String, String>> splitByKcqlStatements(int maxTasks, Map<String, String> props,
      KcqlSettings kcqlSettings) {
    val kcqlSettingsKey = kcqlSettings.getKcqlSettingsKey();
    val kcqls =
        Arrays
            .stream(props.get(kcqlSettingsKey).split(KCQL_MULTI_STATEMENT_SEPARATOR))
            .collect(Collectors.toList());

    return ListSplitter
        .splitList(kcqls, maxTasks)
        .stream()
        .map(kcqlsForTask -> Stream.concat(
            props.entrySet().stream(),
            Stream.of(Map.entry(kcqlSettingsKey, String.join(";", kcqlsForTask)))
        ).collect(Collectors.toUnmodifiableMap(
            Map.Entry::getKey,
            Map.Entry::getValue,
            (existing, replacement) -> replacement
        )))
        .collect(Collectors.toUnmodifiableList());
  }

  /**
   * Replicates the same properties map for all tasks.
   * This allows all tasks to receive the same configuration, enabling parallel consumption
   * from the same sources (e.g., GCP Pub/Sub subscriptions) with native load-balancing.
   *
   * @param maxTasks the number of tasks to create
   * @param props    the properties map to replicate
   * @return a list containing {@code maxTasks} copies of the properties map
   */
  public static List<Map<String, String>> replicateForAllTasks(int maxTasks, Map<String, String> props) {
    if (maxTasks <= 0) {
      return Collections.emptyList();
    }
    return IntStream.range(0, maxTasks)
        .mapToObj(i -> Map.copyOf(props))
        .collect(Collectors.toUnmodifiableList());
  }

}

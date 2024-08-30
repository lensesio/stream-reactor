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
package io.lenses.streamreactor.common.validation.validators;

import io.lenses.kcql.Kcql;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Checks if all {@link Kcql}s have distinct Source names.
 */
public class DistinctSourceNamesValidator implements AllKcqlValidator {

  @Override
  public List<String> validate(List<Kcql> kcqls) {
    List<String> sources = kcqls.stream().map(Kcql::getSource).collect(Collectors.toUnmodifiableList());
    return detectDuplicateTopicMappings(sources).stream()
        .map(dupe -> String.format("Source '%s' cannot be mapped twice.", dupe))
        .collect(Collectors.toUnmodifiableList());
  }

  private static List<String> detectDuplicateTopicMappings(List<String> topics) {
    return topics.stream()
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
        .entrySet().stream()
        .filter(entry -> entry.getValue() > 1)
        .map(Map.Entry::getKey)
        .collect(Collectors.toUnmodifiableList());
  }
}

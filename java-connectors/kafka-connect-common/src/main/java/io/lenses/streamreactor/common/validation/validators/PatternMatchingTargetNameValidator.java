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
package io.lenses.streamreactor.common.validation.validators;

import io.lenses.kcql.Kcql;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

public class PatternMatchingTargetNameValidator implements SingleKcqlValidator {

  private final UnaryOperator<String> topicErrorMessageFunction;

  private final Pattern topicNamePattern;

  public PatternMatchingTargetNameValidator(String topicNameRegex, UnaryOperator<String> topicErrorMessageFunction) {
    this.topicErrorMessageFunction = topicErrorMessageFunction;
    this.topicNamePattern = Pattern.compile(topicNameRegex);
  }

  @Override
  public List<String> validate(Kcql kcql) {
    String kcqlTarget = kcql.getTarget();
    return topicNamePattern.matcher(kcqlTarget).matches() ? Collections.emptyList() : List.of(topicErrorMessageFunction
        .apply(kcqlTarget));
  }
}

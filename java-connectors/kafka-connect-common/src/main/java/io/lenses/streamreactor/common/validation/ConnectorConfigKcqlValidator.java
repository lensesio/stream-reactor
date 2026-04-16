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
package io.lenses.streamreactor.common.validation;

import static io.lenses.streamreactor.common.util.StringUtils.getSystemsNewLineChar;

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.common.validation.validators.AllKcqlValidator;
import io.lenses.streamreactor.common.validation.validators.SingleKcqlValidator;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;

@Builder
@AllArgsConstructor
public class ConnectorConfigKcqlValidator {

  private static final String ERROR_DELIMITER = ";" + getSystemsNewLineChar();
  @Builder.Default
  private final List<SingleKcqlValidator> singleKcqlValidators = Collections.emptyList();
  @Builder.Default
  private final List<AllKcqlValidator> allKcqlValidators = Collections.emptyList();
  @Builder.Default
  private final Function<Collection<String>, StreamReactorException> exceptionFormatter =
      STRING_JOINING_EXCEPTION_FORMATTER;

  public static final Function<Collection<String>, StreamReactorException> STRING_JOINING_EXCEPTION_FORMATTER =
      allErrors -> new StreamReactorException(String.format("The following errors occurred during validation: %s",
          String.join(ERROR_DELIMITER, allErrors))
      );

  public Either<StreamReactorException, List<Kcql>> validateKcqlString(String kcqlString) {
    List<Kcql> kcqls = Kcql.parseMultiple(kcqlString);
    Set<String> allErrors = new HashSet<>();

    singleKcqlValidators.stream()
        .map(validator -> kcqls.stream().map(validator::validate).collect(Collectors.toUnmodifiableList()))
        .flatMap(nestedErrors -> nestedErrors.stream().flatMap(Collection::stream)).forEach(allErrors::add);
    allKcqlValidators.stream()
        .map(validator -> validator.validate(kcqls)).flatMap(Collection::stream).forEach(allErrors::add);

    if (!allErrors.isEmpty()) {
      return Either.left(exceptionFormatter.apply(allErrors));
    }

    return Either.right(List.copyOf(kcqls));
  }

}

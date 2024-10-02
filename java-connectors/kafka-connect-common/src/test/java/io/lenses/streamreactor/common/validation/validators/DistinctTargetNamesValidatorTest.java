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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.lenses.kcql.Kcql;
import java.util.List;
import org.junit.jupiter.api.Test;

class DistinctTargetNamesValidatorTest {

  @Test
  void validateNonDuplicatedKcqlSourcesDontProduceErrors() {
    //given
    DistinctTargetNamesValidator validator = new DistinctTargetNamesValidator();
    List<Kcql> validKcqls =
        Kcql.parseMultiple("INSERT INTO abc SELECT * FROM xyz; INSERT INTO def SELECT * FROM zyx;");

    //when
    List<String> errorList = validator.validate(validKcqls);

    //then
    assertThat(errorList).isEmpty();
  }

  @Test
  void validateDuplicatedKcqlSourcesProduceErrors() {
    //given
    final String sourceDuplicatedError = "Target 'abc' cannot be mapped twice.";

    DistinctTargetNamesValidator validator = new DistinctTargetNamesValidator();
    List<Kcql> invalidKcqls =
        Kcql.parseMultiple("INSERT INTO abc SELECT * FROM xyz; INSERT INTO abc SELECT * FROM zyx;");

    //when
    List<String> errorList = validator.validate(invalidKcqls);

    //then
    assertThat(errorList).hasSize(1);
    assertEquals(errorList.get(0), sourceDuplicatedError);
  }
}

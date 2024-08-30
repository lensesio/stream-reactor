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
package io.lenses.streamreactor.common.validation;

import static io.lenses.streamreactor.common.util.StringUtils.getSystemsNewLineChar;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import cyclops.control.Either;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import io.lenses.streamreactor.common.validation.validators.AllKcqlValidator;
import io.lenses.streamreactor.common.validation.validators.SingleKcqlValidator;
import io.lenses.streamreactor.test.utils.EitherValues;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ConnectorConfigKcqlValidatorTest {

  private static final String STRING_KCQL = "INSERT INTO abc SELECT * FROM xyz;";
  private static final String ERROR_MSG = "SOME_ERROR";
  private static final String FULL_ERROR_MSG =
      "The following errors occurred during validation: "
          + ERROR_MSG;

  @Test
  void validateKcqlStringShouldProduceListOfErrorsIfAtLeastOneOfSingleKcqlValidatorsProducesThem() {
    //given
    SingleKcqlValidator failingSingleKcqlValidator = mock(SingleKcqlValidator.class);
    when(failingSingleKcqlValidator.validate(any(Kcql.class))).thenReturn(List.of(ERROR_MSG));

    ConnectorConfigKcqlValidator configKcqlValidator =
        ConnectorConfigKcqlValidator.builder()
            .singleKcqlValidators(List.of(failingSingleKcqlValidator)).build();

    //when
    Either<StreamReactorException, List<Kcql>> lists = configKcqlValidator.validateKcqlString(STRING_KCQL);

    //then
    StreamReactorException left = EitherValues.getLeft(lists);
    assertEquals(FULL_ERROR_MSG, left.getMessage());
  }

  @Test
  void validateKcqlStringShouldProduceListOfErrorsIfAtLeastOneOfAllKcqlValidatorsProducesThem() {
    //given
    AllKcqlValidator failingAllKcqlValidator = mock(AllKcqlValidator.class);
    when(failingAllKcqlValidator.validate(anyList())).thenReturn(List.of(ERROR_MSG));

    ConnectorConfigKcqlValidator configKcqlValidator =
        ConnectorConfigKcqlValidator.builder()
            .allKcqlValidators(List.of(failingAllKcqlValidator)).build();

    //when
    Either<StreamReactorException, List<Kcql>> lists = configKcqlValidator.validateKcqlString(STRING_KCQL);

    //then
    StreamReactorException left = EitherValues.getLeft(lists);
    assertEquals(FULL_ERROR_MSG, left.getMessage());
  }

  @Test
  void validateKcqlStringShouldAggregateErrorMessagesIfValidatorsProducesThem() {
    //given
    String anotherError = "ANOTHER_ERROR";
    String fullErrorMsg =
        "The following errors occurred during validation: "
            + anotherError + ";" + getSystemsNewLineChar() + ERROR_MSG;

    AllKcqlValidator failingAllKcqlValidator = mock(AllKcqlValidator.class);
    when(failingAllKcqlValidator.validate(anyList())).thenReturn(List.of(ERROR_MSG));

    SingleKcqlValidator failingSingleKcqlValidator = mock(SingleKcqlValidator.class);
    when(failingSingleKcqlValidator.validate(any(Kcql.class))).thenReturn(List.of(anotherError));

    ConnectorConfigKcqlValidator configKcqlValidator =
        ConnectorConfigKcqlValidator.builder()
            .singleKcqlValidators(List.of(failingSingleKcqlValidator))
            .allKcqlValidators(List.of(failingAllKcqlValidator)).build();

    //when
    Either<StreamReactorException, List<Kcql>> lists = configKcqlValidator.validateKcqlString(STRING_KCQL);

    //then
    StreamReactorException left = EitherValues.getLeft(lists);
    assertEquals(fullErrorMsg, left.getMessage());
  }

  @Test
  void validateKcqlStringShouldProduceKcqlListIfNoErrorsRaisedByValidators() {
    //given
    AllKcqlValidator failingAllKcqlValidator = mock(AllKcqlValidator.class);
    when(failingAllKcqlValidator.validate(anyList())).thenReturn(Collections.emptyList());

    SingleKcqlValidator failingSingleKcqlValidator = mock(SingleKcqlValidator.class);
    when(failingSingleKcqlValidator.validate(any(Kcql.class))).thenReturn(Collections.emptyList());

    ConnectorConfigKcqlValidator configKcqlValidator =
        ConnectorConfigKcqlValidator.builder()
            .singleKcqlValidators(List.of(failingSingleKcqlValidator))
            .allKcqlValidators(List.of(failingAllKcqlValidator)).build();

    //when
    Either<StreamReactorException, List<Kcql>> lists = configKcqlValidator.validateKcqlString(STRING_KCQL);

    //then
    List<Kcql> kcqls = EitherValues.getRight(lists);
    assertThat(kcqls).hasSize(1);
  }
}

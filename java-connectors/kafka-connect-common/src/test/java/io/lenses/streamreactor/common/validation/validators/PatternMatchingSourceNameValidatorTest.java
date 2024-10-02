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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.kcql.Kcql;
import java.util.List;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class PatternMatchingSourceNameValidatorTest {

  private static final String ONLY_LETTERS_REGEXP = "^[A-Za-z]*$";
  private static final UnaryOperator<String> ERROR_MESSAGE_FUNCTION = mock(UnaryOperator.class);

  @Test
  void validateShouldReturnEmptyErrorListIfSourceTopicMatchesRegexp() {
    //given
    PatternMatchingSourceNameValidator validator =
        new PatternMatchingSourceNameValidator(ONLY_LETTERS_REGEXP, ERROR_MESSAGE_FUNCTION);
    Kcql validKcql = Kcql.parse("INSERT INTO abc SELECT * FROM def");

    //when
    List<String> errorList = validator.validate(validKcql);

    //then
    assertThat(errorList).isEmpty();
    verify(ERROR_MESSAGE_FUNCTION, times(0)).apply(any(String.class));
  }

  @Test
  void validateShouldReturnListWithErrorIfSourceTopicDoesntMatchRegexp() {
    //given
    String errorString = "ERROR";
    PatternMatchingSourceNameValidator validator =
        new PatternMatchingSourceNameValidator(ONLY_LETTERS_REGEXP, ERROR_MESSAGE_FUNCTION);
    Kcql invalidKcql = Kcql.parse("INSERT INTO abc SELECT * FROM d1ef");
    when(ERROR_MESSAGE_FUNCTION.apply(anyString())).thenReturn(errorString);

    //when
    List<String> errorList = validator.validate(invalidKcql);

    //then
    assertThat(errorList).hasSize(1);
    verify(ERROR_MESSAGE_FUNCTION).apply(any(String.class));
  }
}

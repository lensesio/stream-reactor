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
package io.lenses.streamreactor.common.util;

import cyclops.control.Either;
import io.lenses.streamreactor.common.exception.StreamReactorException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EitherUtilsTest {

  @Test
  void testUnpackOrThrowWithRightValue() {
    Either<StreamReactorException, String> either = Either.right("success");
    String result = EitherUtils.unpackOrThrow(either);
    assertEquals("success", result);
  }

  @Test
  void testUnpackOrThrowWithLeftValue() {
    Either<StreamReactorException, String> either = Either.left(new StreamReactorException("error"));
    assertThrows(ConnectException.class, () -> EitherUtils.unpackOrThrow(either));
  }

  @Test
  void testUnpackOrThrowWithCustomExceptionSupplierAndRightValue() {
    Either<StreamReactorException, String> either = Either.right("success");
    Function<StreamReactorException, ConnectException> exceptionSupplier = ConnectException::new;
    String result = EitherUtils.unpackOrThrow(either, exceptionSupplier);
    assertEquals("success", result);
  }

  @Test
  void testUnpackOrThrowWithCustomExceptionSupplierAndLeftValue() {
    StreamReactorException exception = new StreamReactorException("error");
    Either<StreamReactorException, String> either = Either.left(exception);
    String exceptionMessage = "Something is amiss";
    Function<StreamReactorException, ConnectException> exceptionSupplier = e -> new DataException(exceptionMessage);

    ConnectException thrownException =
        assertThrows(ConnectException.class, () -> EitherUtils.unpackOrThrow(either, exceptionSupplier));
    assertEquals(exceptionMessage, thrownException.getMessage());
  }

}

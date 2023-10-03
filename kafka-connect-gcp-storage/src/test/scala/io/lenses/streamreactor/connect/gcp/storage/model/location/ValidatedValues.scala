/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.storage.model.location

import cats.data.Validated
import org.scalatest.Assertion
import org.scalatest.Assertions
import org.scalatest.matchers.should.Matchers

trait ValidatedValues extends Assertions with Matchers {

  implicit class ValidatedOps[E, A](validated: Validated[E, A]) {
    def isValid: Assertion =
      assert(validated.isValid, s"Expected Valid, but got Invalid($validated)")

    def isInvalid: Assertion =
      assert(validated.isInvalid, s"Expected Invalid, but got Valid($validated)")

    def value: A =
      validated.getOrElse(throw new NoSuchElementException("Validated is Invalid"))

    def leftValue: E =
      validated.swap.getOrElse(throw new NoSuchElementException("Validated is Valid"))
  }

}

object ValidatedValues extends ValidatedValues

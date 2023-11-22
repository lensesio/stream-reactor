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
package io.lenses.streamreactor.connect.datalake.model.location

import cats.data.Validated
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DatalakeLocationValidatorTest extends AnyFunSuite with Matchers {

  private implicit val validator: DatalakeLocationValidator.type = DatalakeLocationValidator

  test("DatalakeLocationValidator should validate a valid bucket name") {
    val location = CloudLocation("valid-bucket-name", none, "valid-path".some)
    val result   = DatalakeLocationValidator.validate(location, allowSlash = false)
    result shouldBe Validated.Valid(location)
  }

  test("DatalakeLocationValidator should return an error for an invalid bucket name") {
    val location = CloudLocation("invalid_bucket_name", none, "valid-path".some)
    val result   = DatalakeLocationValidator.validate(location, allowSlash = false)
    result shouldBe a[Validated.Invalid[_]]
  }

  test("DatalakeLocationValidator should return an error if prefix contains a slash when not allowed") {
    val location = CloudLocation("valid-bucket-name", "prefix/".some, "valid-path".some)
    val result   = DatalakeLocationValidator.validate(location, allowSlash = false)
    result shouldBe a[Validated.Invalid[_]]
  }

  test("DatalakeLocationValidator should allow prefix with a slash when allowed") {
    val location = CloudLocation("valid-bucket-name", "prefix/".some, "valid-path".some)
    val result   = DatalakeLocationValidator.validate(location, allowSlash = true)
    result shouldBe Validated.Valid(location)
  }
}

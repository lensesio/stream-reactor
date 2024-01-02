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
package io.lenses.streamreactor.connect.gcp.storage.model.location

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData.cloudLocationValidator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GCPStorageLocationValidatorTest extends AnyFunSuite with Matchers with ValidatedValues {

  test("validate should succeed for a valid CloudLocation") {
    val validLocation = CloudLocation("valid-bucket", Some("valid-prefix"))
    val result        = GCPStorageLocationValidator.validate(validLocation)
    result.value should be(validLocation)
  }

  test("validate should fail for an invalid bucket name") {
    val invalidLocation = CloudLocation("invalid@bucket", Some("valid-prefix"))
    val result: Validated[Throwable, CloudLocation] =
      GCPStorageLocationValidator.validate(invalidLocation)
    result.leftValue.getMessage should be("Nested prefix not currently supported")
  }

  test("validate should fail for a prefix with slashes") {
    val invalidLocation = CloudLocation("valid-bucket", Some("slash/prefix"))
    val result: Validated[Throwable, CloudLocation] =
      GCPStorageLocationValidator.validate(invalidLocation)
    result.value should be(CloudLocation("valid-bucket", Some("slash/prefix")))
  }

  test("validate should succeed for a valid prefix with slashes not allowed") {
    val validLocation = CloudLocation("valid-bucket", Some("valid-prefix"))
    val result: Validated[Throwable, CloudLocation] =
      GCPStorageLocationValidator.validate(validLocation)
    result.value should be(validLocation)
  }

  test("validate should succeed for a valid prefix with slashes allowed") {
    val validLocation = CloudLocation("valid-bucket", Some("valid-prefix"))
    val result: Validated[Throwable, CloudLocation] =
      GCPStorageLocationValidator.validate(validLocation)
    result.value should be(validLocation)
  }
}

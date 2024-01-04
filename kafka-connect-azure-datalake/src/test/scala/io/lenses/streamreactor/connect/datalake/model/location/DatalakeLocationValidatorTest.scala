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
package io.lenses.streamreactor.connect.datalake.model.location

import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class DatalakeLocationValidatorTest
    extends AnyFunSuite
    with Matchers
    with ValidatedValues
    with ScalaCheckPropertyChecks {

  private val validBucketNames =
    Table(
      "bucketName",
      "abc123",
      "container1",
      "name-123",
      "a-b-c-4",
      "x12",
      "my-container-99",
    )

  private val invalidBucketNames =
    Table(
      ("bucketName", "prompt"),
      ("-abc", "Invalid bucket name"),
      ("container!", "Invalid bucket name"),
      ("name--123", "Invalid bucket name"),
      ("AaBbCc", "Invalid bucket name"),
      ("my container", "Invalid bucket name"),
      ("12345678901234567890123456789012345678901234567890123456789012345", "Invalid bucket name"),
      ("ab_c", "Invalid bucket name"),
      ("x1", "Invalid bucket name"),
    )
  private implicit val validator: DatalakeLocationValidator.type = DatalakeLocationValidator

  forAll(validBucketNames) {
    bN: String =>
      test(s"allow valid bucket name : $bN") {

        val validLocation = CloudLocation(bN, Some("valid-prefix"))
        val result: Validated[Throwable, CloudLocation] =
          DatalakeLocationValidator.validate(validLocation)
        result.value should be(validLocation)
      }
  }

  forAll(invalidBucketNames) {
    (bN: String, prompt: String) =>
      test(s"disallow invalid bucket names : $bN") {

        val validLocation = CloudLocation(bN, Some("valid-prefix"))
        val result: Validated[Throwable, CloudLocation] =
          DatalakeLocationValidator.validate(validLocation)
        result.leftValue.getMessage should startWith("Invalid bucket name")
        result.leftValue.getMessage should endWith(prompt)
      }
  }
}

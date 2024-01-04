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
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class GCPStorageLocationValidatorTest
    extends AnyFunSuite
    with Matchers
    with ValidatedValues
    with ScalaCheckPropertyChecks {

  private val validBucketNames =
    Table(
      "bucketName",
      "my-travel-maps",
      "0f75d593-8e7b-4418-a5ba-cb2970f0b91e",
      "valid_bucket_name",
      "dot.valid.bucket.name",
      "123",
    )

  private val invalidBucketNames =
    Table(
      ("bucketName", "prompt"),
      ("My-Travel-Maps", "Bucket name should match regex"),
      ("my_google_bucket", "Bucket name cannot contain 'google' or variants"),
      ("test bucket", "Bucket name should match regex"),
      ("invalid bucket name with space", "Bucket name should match regex"),
      ("192.168.5.4", "Bucket name should not be an IP address"),
      ("goog_bucket", "Bucket name cannot contain 'google' or variants"),
      ("g00gle_bucket", "Bucket name cannot contain 'google' or variants"),
      ("test_bucket_name_with_65_characters_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "Rule: Bucket name should match regex"),
      ("test_bucket.name.with.225.characters.xxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
       "Bucket name containing dots should be less than 222 characters",
      ),
    )

  forAll(validBucketNames) {
    bN: String =>
      test(s"allow valid bucket name : $bN") {

        val validLocation = CloudLocation(bN, Some("valid-prefix"))
        val result: Validated[Throwable, CloudLocation] =
          GCPStorageLocationValidator.validate(validLocation)
        result.value should be(validLocation)
      }
  }

  forAll(invalidBucketNames) {
    (bN: String, prompt: String) =>
      test(s"disallow invalid bucket names : $bN") {

        val validLocation = CloudLocation(bN, Some("valid-prefix"))
        val result: Validated[Throwable, CloudLocation] =
          GCPStorageLocationValidator.validate(validLocation)
        result.leftValue.getMessage should startWith("Invalid bucket name")
        result.leftValue.getMessage should endWith(prompt)
      }
  }

}

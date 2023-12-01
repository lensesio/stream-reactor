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
package io.lenses.streamreactor.connect.aws.s3.sink.config

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.consumers.CloudObjectKey
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class S3ObjectKeyValidationTest extends AnyFunSuite with Matchers {
  test("S3ObjectKey should be valid") {
    val bucket = CloudObjectKey("bucket", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe true
  }
  test("bucket with . and -  is valid") {
    S3ObjectKey.validate(CloudObjectKey("buck.et", "prefix".some)).isValid shouldBe true
    S3ObjectKey.validate(CloudObjectKey("buck-et", "prefix".some)).isValid shouldBe true
  }
  test("bucket shorter than 3 characters should be invalid") {
    val bucket = CloudObjectKey("bu", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket longer than 63 characters should be invalid") {
    val bucket = CloudObjectKey("b" * 64, "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket with uppercase characters should be invalid") {
    val bucket = CloudObjectKey("BUCKET", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket with invalid characters should be invalid") {
    val bucket = CloudObjectKey("buck et", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket with invalid prefix should be invalid") {
    val bucket = CloudObjectKey("bucket", "pre fix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket starting with anything but a letter or number should be invalid") {
    val bucket = CloudObjectKey(".bucket", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket ending with anything but a letter or number should be invalid") {
    val bucket = CloudObjectKey("bucket.", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket starting with sthree- is invalid") {
    val bucket = CloudObjectKey("sthree-bucket", "aws".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket starting with xn-- is invalid") {
    val bucket = CloudObjectKey("xn--bucket", "aws".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("bucket with two adjacent periods is invalid") {
    val bucket = CloudObjectKey("buck..et", "prefix".some)
    S3ObjectKey.validate(bucket).isValid shouldBe false
  }
  test("valid prefix is valid") {
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab.c/x/y/z".some)).isValid shouldBe true
    S3ObjectKey.validate(CloudObjectKey("bucket", "4a/b/c".some)).isValid shouldBe true
  }
  test("prefix starting with / or ending with / is invalid") {
    S3ObjectKey.validate(CloudObjectKey("bucket", "/ab/c".some)).isValid shouldBe false
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab/c/".some)).isValid shouldBe false
  }
  test("prefix with characters other than 0-9,a-z,A-Z,!, - , _, ., *, ', ), (, and / is invalid") {
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab/c$".some)).isValid shouldBe false
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab/c%".some)).isValid shouldBe false
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab/c&".some)).isValid shouldBe false
    S3ObjectKey.validate(CloudObjectKey("bucket", "ab/c?".some)).isValid shouldBe false
    S3ObjectKey.validate(CloudObjectKey("bucket", "@ab/c?".some)).isValid shouldBe false
  }
}

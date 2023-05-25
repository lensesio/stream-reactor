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
package io.lenses.streamreactor.connect.aws.s3.model

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3LocationTest extends AnyFlatSpec with Matchers with EitherValues {

  "bucketAndPrefix" should "reject prefixes with slashes" in {
    expectException(
      S3Location.createAndValidate("bucket", "/slash", allowSlash = false),
      "Nested prefix not currently supported",
    )
  }

  private def expectException(response: Either[Throwable, S3Location], expectedMessage: String): Unit =
    response.left.value match {
      case ex: IllegalArgumentException => ex.getMessage should be(expectedMessage)
        ()
      case _ => fail("Unexpected error message")
    }

  "bucketAndPrefix" should "allow prefixes without slashes" in {
    S3Location.createAndValidate("bucket", "noSlash", allowSlash = false)
  }

  "bucketAndPrefix" should "split the bucket and prefix" in {
    S3Location.splitAndValidate("bucket:prefix", allowSlash = false).value should be(S3Location("bucket",
                                                                                                "prefix".some,
    ))
  }

  "bucketAndPrefix" should "fail if given too many components to split" in {
    expectException(
      S3Location.splitAndValidate("bucket:path:whatIsThis", false),
      "Invalid number of arguments provided to create BucketAndPrefix",
    )
  }

  "bucketAndPrefix" should "fail if not a valid bucket name" in {
    expectException(
      S3Location.splitAndValidate("bucket-police-refu$e-this-name:path", allowSlash = true),
      "Bucket name should not contain '$'",
    )
  }

}

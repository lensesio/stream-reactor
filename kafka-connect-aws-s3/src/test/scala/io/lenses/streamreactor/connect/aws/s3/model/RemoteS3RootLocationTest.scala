
/*
 * Copyright 2020 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RemoteS3RootLocationTest extends AnyFlatSpec with Matchers {

  "bucketAndPrefix" should "reject prefixes with slashes" in {
    assertThrows[IllegalArgumentException] {
      RemoteS3RootLocation("bucket", Some("/slash"))
    }
  }

  "bucketAndPrefix" should "allow prefixes without slashes" in {
    RemoteS3RootLocation("bucket", Some("noSlash"))
  }

  "bucketAndPrefix" should "split a the bucket and path" in {
    RemoteS3RootLocation("bucket:path") should be(RemoteS3RootLocation("bucket", Some("path")))
  }

  "bucketAndPrefix" should "fail if given too many components to split" in {
    assertThrows[IllegalArgumentException] {
      RemoteS3RootLocation("bucket:path:whatIsThis")
    }
  }

  "bucketAndPrefix" should "fail if not a valid bucket name" in {
    assertThrows[IllegalArgumentException] {
      RemoteS3RootLocation("bucket-police-refu$e-this-name:path")
    }
  }
}

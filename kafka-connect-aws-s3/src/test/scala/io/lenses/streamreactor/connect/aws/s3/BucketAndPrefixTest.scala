
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

package io.lenses.streamreactor.connect.aws.s3

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BucketAndPrefixTest extends AnyFlatSpec with Matchers {

  "bucketAndPrefix" should "reject prefixes with slashes" in {
    assertThrows[IllegalArgumentException] {
      BucketAndPrefix("bucket", Some("/slash"))
    }
  }

  "bucketAndPrefix" should "allow prefixes without slashes" in {
    BucketAndPrefix("bucket", Some("noslash"))
  }

  "bucketAndPrefix" should "split a the bucket and path" in {
    BucketAndPrefix("bucket:path") should be (BucketAndPrefix("bucket", Some("path")))
  }

  "bucketAndPrefix" should "fail if given too many components to split" in {
    assertThrows[IllegalArgumentException] {
      BucketAndPrefix("bucket:path:whatisthis")
    }
  }

}

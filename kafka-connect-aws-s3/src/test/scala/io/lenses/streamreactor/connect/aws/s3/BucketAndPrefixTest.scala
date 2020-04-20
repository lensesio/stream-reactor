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

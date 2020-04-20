package io.lenses.streamreactor.connect.aws.s3.sink

import java.util.UUID

import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import org.jclouds.blobstore.domain.Blob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SmokeTest extends AnyFlatSpec with Matchers with S3TestConfig {

  "smoke test" should "be able to write directly to s3 proxy" in {

    val blob: Blob = blobStoreContext.getBlobStore.blobBuilder("testblob")
      .payload("<html><body>hello world2</body></html>")
      .contentType("text/html")
      .eTag(UUID.randomUUID().toString)
      .build()

    val eTag = blobStoreContext.getBlobStore.putBlob(BucketName, blob)

    eTag should not be empty

  }
}

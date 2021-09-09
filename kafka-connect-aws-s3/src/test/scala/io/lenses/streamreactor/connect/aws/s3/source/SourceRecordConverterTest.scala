package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SourceRecordConverterTest extends AnyFlatSpec with Matchers {

  "fromSourcePartition" should "convert RemoteS3RootLocation to Map" in {
    SourceRecordConverter.fromSourcePartition(RemoteS3RootLocation("test-bucket:test-prefix")) should contain allOf(
      "container" -> "test-bucket",
      "prefix" -> "test-prefix"
    )
  }

  "fromSourcePartition" should "convert RemoteS3RootLocation without prefix to Map" in {
    SourceRecordConverter.fromSourcePartition(RemoteS3RootLocation("test-bucket")) should contain allOf(
      "container" -> "test-bucket",
      "prefix" -> ""
    )
  }

  "fromSourceOffset" should "convert RemoteS3RootLocation to Map" in {
    SourceRecordConverter.fromSourceOffset(
      RemoteS3RootLocation("test-bucket:test-prefix").withPath("test-path"),
      100L
    ) should contain allOf(
      "path" -> "test-path",
      "line" -> "100"
    )
  }

}

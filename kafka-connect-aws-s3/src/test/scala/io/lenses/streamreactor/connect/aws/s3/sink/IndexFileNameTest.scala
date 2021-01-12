package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.model.{Offset, Topic}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class IndexFileNameTest extends AnyFlatSpecLike with Matchers {

  val indexNamingStrategy = new PartitionedS3IndexNamingStrategy

  "unapply" should "recognise index filenames in prefix/topic/927/latest.77 format" in {
    IndexFileName.from("index/topic/927/latest.77", indexNamingStrategy) shouldBe Some(IndexFileName(Topic("topic"), 927, Offset(77)))
  }

  "unapply" should "not recognise non index filenames" in {
    IndexFileName.from("index/topic/927/77", indexNamingStrategy) shouldBe None
  }
}

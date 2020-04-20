package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.{Offset, Topic}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class CommittedFileNameTest extends AnyFlatSpecLike with Matchers {

  "unapply" should "recognise filenames in prefix/topic/927/77.json format" in {
    CommittedFileName.unapply("prefix/topic/927/77.json") should be(Some("prefix", Topic("topic"), 927, Offset(77), Json))
  }

  "unapply" should "not recognise filenames other formats" in {
    CommittedFileName.unapply("prefix/topic/927/77") should be(None)
  }

  "unapply" should "not recognise filenames for non-supported file types" in {
    CommittedFileName.unapply("prefix/topic/927/77.doc") should be(None)
  }

  "unapply" should "not recognise filenames for a long path" in {
    CommittedFileName.unapply("extra/long/prefix/topic/927/77.doc") should be(None)
  }
}


package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.{Offset, Topic, TopicPartitionOffset}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DefaultCommitPolicyTest extends AnyWordSpec with Matchers {

  private def shouldFlush(
                           policy: CommitPolicy,
                           count: Long,
                           fileSize: Long,
                           timestampAdjuster: Long => Long = nowTime => nowTime
                         ) = {

    val nowTime = System.currentTimeMillis()
    val timeAdjusted = timestampAdjuster(nowTime)
    val tpo = TopicPartitionOffset(Topic("mytopic"), 1, Offset(100))

    policy.shouldFlush(CommitContext(tpo, count, fileSize, timeAdjusted))
  }

  "DefaultCommitPolicy" should {

    "roll over after interval" in {

      val policy = DefaultCommitPolicy(None, Option(2.seconds), None)

      shouldFlush(policy, 10, 0) shouldBe false
      shouldFlush(policy, 10, 0, _ - 2000) shouldBe true
    }

    "roll over after file count" in {
      val policy = DefaultCommitPolicy(None, None, Some(9))

      shouldFlush(policy, 7, 0) shouldBe false
      shouldFlush(policy, 8, 0) shouldBe false
      shouldFlush(policy, 9, 0) shouldBe true
      shouldFlush(policy, 10, 0) shouldBe true
    }

    "roll over after file size" in {
      val policy = DefaultCommitPolicy(Some(10), None, None)

      shouldFlush(policy, 7, 0) shouldBe false
      shouldFlush(policy, 7, 10) shouldBe true
    }
  }
}

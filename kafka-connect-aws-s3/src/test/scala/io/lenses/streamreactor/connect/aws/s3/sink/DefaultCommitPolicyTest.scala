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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration._

class DefaultCommitPolicyTest extends AnyWordSpec with Matchers {

  private def shouldFlush(
    policy:                    CommitPolicy,
    count:                     Long,
    fileSize:                  Long,
    creationTimestampAdjuster: Long => Long = nowTime => nowTime,
    lastFlushTimestampAdjust:  Option[Long] = None,
  ) = {

    val nowTime              = System.currentTimeMillis()
    val creationTimeAdjusted = creationTimestampAdjuster(nowTime)
    val lastFlushTimeAdjusted: Option[Long] = lastFlushTimestampAdjust.fold(Option.empty[Long])(e => Some(nowTime + e))
    val tpo = TopicPartitionOffset(Topic("myTopic"), 1, Offset(100))

    policy.shouldFlush(CommitContext(tpo, count, fileSize, creationTimeAdjusted, lastFlushTimeAdjusted))
  }

  "DefaultCommitPolicy" should {

    "roll over after interval from file creation" in {

      val policy = DefaultCommitPolicy(None, Option(2.seconds), None)

      shouldFlush(policy, 10, 0) shouldBe false
      shouldFlush(policy, 10, 0, _ - 2000) shouldBe true
    }

    "roll over after interval from last flush" in {

      val policy = DefaultCommitPolicy(None, Option(2.seconds), None)

      shouldFlush(policy, 10, 0) shouldBe false
      shouldFlush(policy, 10, 0, lastFlushTimestampAdjust = Some(-2000)) shouldBe true
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

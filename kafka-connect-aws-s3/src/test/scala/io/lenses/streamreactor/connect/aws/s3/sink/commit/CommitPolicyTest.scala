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
package io.lenses.streamreactor.connect.aws.s3.sink.commit

import com.typesafe.scalalogging.Logger
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{ Logger => Slf4jLogger }

import scala.concurrent.duration._

class CommitPolicyTest extends AnyWordSpec with Matchers with MockitoSugar {

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

    policy.shouldFlush(
      CommitContext(tpo,
                    count,
                    fileSize,
                    creationTimeAdjusted,
                    lastFlushTimeAdjusted,
                    "my/filename.txt",
                    ConnectorTaskId("connector", 1, 0),
      ),
    )
  }

  "CommitPolicy" should {

    "roll over after interval from file creation" in {

      val policy = CommitPolicy(Interval(2.seconds))

      shouldFlush(policy, 10, 0) shouldBe false
      shouldFlush(policy, 10, 0, _ - 2000) shouldBe true
    }

    "roll over after interval from last flush" in {

      val policy = CommitPolicy(Interval(2.seconds))

      shouldFlush(policy, 10, 0) shouldBe false
      shouldFlush(policy, 10, 0, lastFlushTimestampAdjust = Some(-2000)) shouldBe true
    }

    "roll over after record count" in {
      val policy = CommitPolicy(Count(9))

      shouldFlush(policy, 7, 0) shouldBe false
      shouldFlush(policy, 8, 0) shouldBe false
      shouldFlush(policy, 9, 0) shouldBe true
      shouldFlush(policy, 10, 0) shouldBe true
    }

    "roll over after file size" in {
      val policy = CommitPolicy(FileSize(10))

      shouldFlush(policy, 7, 0) shouldBe false
      shouldFlush(policy, 7, 10) shouldBe true
    }

    "multiple conditions logging" in {
      val policy = CommitPolicy(Interval(1.seconds), Count(100), FileSize(10))

      shouldFlush(policy, 7, 10) shouldBe true
    }
  }

}

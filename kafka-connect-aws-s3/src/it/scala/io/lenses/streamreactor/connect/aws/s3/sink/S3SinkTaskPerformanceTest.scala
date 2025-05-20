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

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.randomUser
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.schema
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class S3SinkTaskPerformanceTest extends S3ProxyContainerTest with TimedTests with Matchers {

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  private val records = (1 to 100000).map {
    k =>
      val user = randomUser()
      toSinkRecord(user, k)
  }.grouped(100)

  private def toSinkRecord(user: Struct, k: Int, topicName: String = TopicName) =
    new SinkRecord(topicName, 1, null, null, schema, user, k.toLong, k, TimestampType.CREATE_TIME)

  "S3SinkTask" should "measure performance" in {
    Resource.make(IO(createSinkTask())) { sinkTask =>
      IO(sinkTask.stop())
    }.use { sinkTask =>
      IO {
        sinkTask.start(
          (defaultProps ++ Map(
            s"$prefix.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` PROPERTIES('${FlushCount.entryName}'=100)",
          )).asJava,
        )
        sinkTask.open(Seq(new TopicPartition(TopicName, 1)).asJava)
        records.foreach { recGroup =>
          sinkTask.put(recGroup.asJava)
        }
        sinkTask.close(Seq(new TopicPartition(TopicName, 1)).asJava)
        sinkTask.stop()

        listBucketPath(BucketName, "streamReactorBackups/").size should be(1000)
      }
    }.unsafeRunSync()
  }

}

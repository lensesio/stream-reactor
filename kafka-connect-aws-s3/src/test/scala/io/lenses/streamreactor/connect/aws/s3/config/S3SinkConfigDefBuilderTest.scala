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

package io.lenses.streamreactor.connect.aws.s3.config

import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

class S3SinkConfigDefBuilderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  val PrefixName = "streamReactorBackups"
  val TopicName = "myTopic"
  val BucketName = "myBucket"

  "apply" should "respect defined properties" in {
    val props = Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")

    val kcql = S3ConfigDefBuilder(None, props.asJava).getKCQL
    kcql should have size 1

    val element = kcql.head

    element.getStoredAs should be("`CSV`")
    element.getWithFlushCount should be(1)
    element.getWithPartitioner should be("Values")
    element.getPartitionBy.asScala.toSet should be(Set("_key"))

  }

  "apply" should "respect default flush settings" in {
    val props = Map(
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values"
    )

    val commitPolicy = S3ConfigDefBuilder(None, props.asJava).commitPolicy(S3ConfigDefBuilder(None, props.asJava).getKCQL.head)

    commitPolicy.recordCount should be (Some(S3FlushSettings.defaultFlushCount))
    commitPolicy.fileSize should be (Some(S3FlushSettings.defaultFlushSize))
    commitPolicy.interval should be (Some(S3FlushSettings.defaultFlushInterval))
  }

  "apply" should "respect disabled flush count" in {
    val props = Map(
      "connect.s3.disable.flush.count" -> true.toString,
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values"
    )

    val commitPolicy = S3ConfigDefBuilder(None, props.asJava).commitPolicy(S3ConfigDefBuilder(None, props.asJava).getKCQL.head)

    commitPolicy.recordCount should be (None)
    commitPolicy.fileSize should be (Some(S3FlushSettings.defaultFlushSize))
    commitPolicy.interval should be (Some(S3FlushSettings.defaultFlushInterval))
  }

  "apply" should "respect custom flush settings" in {
    val props = Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_SIZE = 3 WITH_FLUSH_INTERVAL = 2 WITH_FLUSH_COUNT = 1")

    val commitPolicy = S3ConfigDefBuilder(None, props.asJava).commitPolicy(S3ConfigDefBuilder(None, props.asJava).getKCQL.head)

    commitPolicy.recordCount should be (Some(1))
    commitPolicy.fileSize should be (Some(3))
    commitPolicy.interval should be (Some(2.seconds))
  }


}
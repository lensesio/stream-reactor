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

import io.lenses.streamreactor.connect.aws.s3.config.CommitMode.Gen1
import io.lenses.streamreactor.connect.aws.s3.config.CommitMode.Gen2
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

class S3SinkConfigDefBuilderTest extends AnyFlatSpec with MockitoSugar with Matchers {

  val PrefixName = "streamReactorBackups"
  val TopicName = "myTopic"
  val BucketName = "mybucket"

  val props = Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")

  "apply" should "respect defined properties" in {
    val kcql = S3ConfigDefBuilder(props.asJava).getKCQL
    kcql should have size 1

    val element = kcql.head

    element.getStoredAs should be("`CSV`")
    element.getWithFlushCount should be(1)
    element.getWithPartitioner should be("Values")
    element.getPartitionBy.asScala.toSet should be(Set("_key"))
  }

  "apply" should "default commit mode" in {
    val config = S3SinkConfig(props)
    config.commitMode shouldBe Gen1
  }

  "apply" should "lift the commit mode for Gen2" in {
    val config = S3SinkConfig(props + (S3ConfigSettings.COMMIT_STRATEGY_CONFIG->"gen2"))
    config.commitMode shouldBe Gen2
  }


  "apply" should "lift the commit mode for Gen1" in {
    val config = S3SinkConfig(props + (S3ConfigSettings.COMMIT_STRATEGY_CONFIG->"gEn1"))
    config.commitMode shouldBe Gen1
  }

  "apply" should "raise an exception when the config for commit mode is incorrect" in {
    intercept[IllegalArgumentException] {
      S3SinkConfig(props + (S3ConfigSettings.COMMIT_STRATEGY_CONFIG -> "not_valid"))
    }
  }
}

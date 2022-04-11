
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

package io.lenses.streamreactor.connect.aws.s3.sink.seek

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, Topic, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfter, EitherValues, OptionValues}

class LegacyOffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers with BeforeAndAfter with EitherValues with OptionValues {

  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val offsetSeeker = new LegacyOffsetSeeker("mySinkName")

  private val topicPartition = Topic("myTopic").withPartition(0)
  private val bucketAndPrefix = RemoteS3RootLocation("my-bucket:path")
  private val bucketAndPath = RemoteS3PathLocation("my-bucket", Some("path"), "path/myTopic/0/")

  after {
    reset(storageInterface)
  }

  "seek" should "return None when path does not exist" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(List().asRight)

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value should be(None)
  }

  "seek" should "return expected offsets for 1 filename" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(List("path/myTopic/0/100.json").asRight)

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(100)),
        bucketAndPrefix.withPath("path/myTopic/0/100.json")
      )
    )

    verify(storageInterface).list(bucketAndPath)
  }

  "seek" should "return highest offset for multiple offsets of the same file" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(
      List("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json").asRight
    )

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)),
        bucketAndPrefix.withPath("path/myTopic/0/300.json")
      )
    )
  }


  "seek" should "return highest offset for multiple offsets of different files" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(
      List("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
        "path/notMyTopic/0/300.json", "path/notMyTopic/0/200.json", "path/notMyTopic/0/100.json").asRight
    )

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)),
        bucketAndPrefix.withPath("path/myTopic/0/300.json")
      )
    )
  }

  "seek" should "ignore other file extensions" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(
      List(
        "path/myTopic/0/100.avro", "path/myTopic/0/200.avro", "path/myTopic/0/300.avro",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      ).asRight
    )

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)),
        bucketAndPrefix.withPath("path/myTopic/0/200.json")
      )
    )
  }

  "seek" should "ignore unknown file extensions" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(
      List(
        "path/myTopic/0/100.doc", "path/myTopic/0/200.xls", "path/myTopic/0/300.ppt",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      ).asRight
    )

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)),
        bucketAndPrefix.withPath("path/myTopic/0/200.json")
      )
    )
  }


  "seek" should "ignore files with no extensions" in {

    when(storageInterface.list(bucketAndPath)).thenReturn(
      List(
        "path/myTopic/0/100", "path/myTopic/0/200", "path/myTopic/0/300",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      ).asRight
    )

    offsetSeeker.seek(topicPartition, fileNamingStrategy, bucketAndPrefix).value.value should be(
      (
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)),
        bucketAndPrefix.withPath("path/myTopic/0/200.json")
      )
    )
  }

}

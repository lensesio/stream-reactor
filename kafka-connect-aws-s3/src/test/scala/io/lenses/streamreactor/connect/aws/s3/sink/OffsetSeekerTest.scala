
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

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPrefix, Offset, Topic, TopicPartitionOffset}
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))
  private val offsetSeeker = new OffsetSeeker(fileNamingStrategy)

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val bucketAndPrefix = BucketAndPrefix("myBucket", Some("path"))

  "seek" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(false)

    offsetSeeker.seek(bucketAndPrefix) should be(Set())
  }

  "seek" should "return expected offsets for 1 filename" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(List("path/myTopic/0/100.json"))

    offsetSeeker.seek(bucketAndPrefix) should be(Set(TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))
  }

  "seek" should "return highest offset for multiple offsets of the same file" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json")
    )

    offsetSeeker.seek(bucketAndPrefix) should be(Set(TopicPartitionOffset(Topic("myTopic"), 0, Offset(300))))
  }


  "seek" should "return highest offset for multiple offsets of different files" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
        "path/notMyTopic/0/300.json", "path/notMyTopic/0/200.json", "path/notMyTopic/0/100.json")
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)),
        TopicPartitionOffset(Topic("notMyTopic"), 0, Offset(300)),
      )
    )
  }

  "seek" should "ignore other file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100.avro", "path/myTopic/0/200.avro", "path/myTopic/0/300.avro",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200))
      )
    )
  }

  "seek" should "ignore unknown file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100.doc", "path/myTopic/0/200.xls", "path/myTopic/0/300.ppt",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200))
      )
    )
  }


  "seek" should "ignore files with no extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100", "path/myTopic/0/200", "path/myTopic/0/300",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(200))
      )
    )
  }

  "seek" should "return offsets for multiple partitions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
        "path/myTopic/1/100.json", "path/myTopic/1/200.json",
        "path/myTopic/2/100.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)),
        TopicPartitionOffset(Topic("myTopic"), 1, Offset(200)),
        TopicPartitionOffset(Topic("myTopic"), 2, Offset(100))
      )
    )
  }
}

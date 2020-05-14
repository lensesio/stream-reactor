
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
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.{BucketAndPrefix, Offset, Topic, TopicPartitionOffset}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(Json)
  private val offsetSeeker = new OffsetSeeker(fileNamingStrategy)

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val bucketAndPrefix = BucketAndPrefix("mybucket", Some("path"))

  "seek" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(false)

    offsetSeeker.seek(bucketAndPrefix) should be(Set())
  }

  "seek" should "return expected offsets for 1 filename" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(List("path/mytopic/0/100.json"))

    offsetSeeker.seek(bucketAndPrefix) should be(Set(TopicPartitionOffset(Topic("mytopic"), 0, Offset(100))))
  }

  "seek" should "return highest offset for multiple offsets of the same file" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List("path/mytopic/0/100.json", "path/mytopic/0/200.json", "path/mytopic/0/300.json")
    )

    offsetSeeker.seek(bucketAndPrefix) should be(Set(TopicPartitionOffset(Topic("mytopic"), 0, Offset(300))))
  }


  "seek" should "return highest offset for multiple offsets of different files" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List("path/mytopic/0/100.json", "path/mytopic/0/200.json", "path/mytopic/0/300.json",
        "path/notmytopic/0/300.json", "path/notmytopic/0/200.json", "path/notmytopic/0/100.json")
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("mytopic"), 0, Offset(300)),
        TopicPartitionOffset(Topic("notmytopic"), 0, Offset(300)),
      )
    )
  }

  "seek" should "ignore other file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/mytopic/0/100.avro", "path/mytopic/0/200.avro", "path/mytopic/0/300.avro",
        "path/mytopic/0/100.json", "path/mytopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("mytopic"), 0, Offset(200))
      )
    )
  }

  "seek" should "ignore unknown file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/mytopic/0/100.doc", "path/mytopic/0/200.xls", "path/mytopic/0/300.ppt",
        "path/mytopic/0/100.json", "path/mytopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("mytopic"), 0, Offset(200))
      )
    )
  }



  "seek" should "ignore files with no extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/mytopic/0/100", "path/mytopic/0/200", "path/mytopic/0/300",
        "path/mytopic/0/100.json", "path/mytopic/0/200.json"
      )
    )

    offsetSeeker.seek(bucketAndPrefix) should be(
      Set(
        TopicPartitionOffset(Topic("mytopic"), 0, Offset(200))
      )
    )
  }
}

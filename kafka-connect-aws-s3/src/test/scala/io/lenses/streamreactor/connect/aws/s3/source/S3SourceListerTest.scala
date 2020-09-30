
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

package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3SourceListerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]

  private val bucketAndPrefix = BucketAndPrefix("myBucket", Some("path"))
  private val sourceLister = new S3SourceLister

  // comes back in random order
  private val defaultJsonFilesTestData = List("path/myTopic/0/200.json", "path/myTopic/0/300.json", "path/myTopic/0/100.json")


  "next" should "return first result when no TopicPartitionOffset has been provided" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(defaultJsonFilesTestData)

    sourceLister.next(fileNamingStrategy, bucketAndPrefix, None, None) should be(
      Some(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100)))
      )
    )
  }

  "next" should "return the second result after we've had the first result" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(defaultJsonFilesTestData)

    sourceLister.next(
      fileNamingStrategy,
      bucketAndPrefix,
      Some(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100)))
      ),
      None
    ) should be(
      Some(
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)))
      )
    )
  }

  "next" should "return empty when we've already had the last result" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(defaultJsonFilesTestData)

    sourceLister.next(fileNamingStrategy, bucketAndPrefix, Some(S3StoredFile("path/myTopic/0/300.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)))), None) should be(
      None
    )
  }


  "list" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(false)

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(List())
  }

  "list" should "return expected offsets for 1 filename" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(List("path/myTopic/0/100.json"))

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be
    List(
      S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100)))
    )
  }

  "list" should "return all offsets in order for multiple offsets of the same file" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    // comes back in random order
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      defaultJsonFilesTestData
    )

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(
      List(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))),
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200))),
        S3StoredFile("path/myTopic/0/300.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(300)))
      )
    )
  }

  "list" should "ignore other file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100.avro", "path/myTopic/0/200.avro", "path/myTopic/0/300.avro",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(
      List(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))),
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)))
      )
    )
  }

  "list" should "ignore unknown file extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100.doc", "path/myTopic/0/200.xls", "path/myTopic/0/300.ppt",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(
      List(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))),
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)))
      )
    )
  }


  "list" should "ignore files with no extensions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/0/100", "path/myTopic/0/200", "path/myTopic/0/300",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json"
      )
    )

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(
      List(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))),
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200)))
      )
    )
  }

  "list" should "return offsets for multiple partitions" in {

    when(storageInterface.pathExists(bucketAndPrefix)).thenReturn(true)
    when(storageInterface.list(bucketAndPrefix)).thenReturn(
      List(
        "path/myTopic/1/100.json", "path/myTopic/1/200.json",
        "path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
        "path/myTopic/2/100.json"
      )
    )

    sourceLister.list(fileNamingStrategy, bucketAndPrefix) should be(
      List(
        S3StoredFile("path/myTopic/0/100.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))),
        S3StoredFile("path/myTopic/0/200.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(200))),
        S3StoredFile("path/myTopic/0/300.json", TopicPartitionOffset(Topic("myTopic"), 0, Offset(300))),

        S3StoredFile("path/myTopic/1/100.json", TopicPartitionOffset(Topic("myTopic"), 1, Offset(100))),
        S3StoredFile("path/myTopic/1/200.json", TopicPartitionOffset(Topic("myTopic"), 1, Offset(200))),

        S3StoredFile("path/myTopic/2/100.json", TopicPartitionOffset(Topic("myTopic"), 2, Offset(100)))
      )
    )
  }
}

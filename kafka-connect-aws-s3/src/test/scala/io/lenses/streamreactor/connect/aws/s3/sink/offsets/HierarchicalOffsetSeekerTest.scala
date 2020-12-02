
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

package io.lenses.streamreactor.connect.aws.s3.sink.offsets

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HierarchicalOffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers {

  private implicit val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))
  private val storageInterface: StorageInterface = mock[StorageInterface]
  
  private val offsetSeeker = new HierarchicalOffsetSeeker(storageInterface)

  private val bucketAndPrefix = BucketAndPrefix("my-bucket", Some("path"))
  private val bucketAndPath = BucketAndPath("my-bucket", "path")
  private val parentBucketAndPath = BucketAndPath("my-bucket", "path/myTopic/0/")
  private val latestBucketAndPath = BucketAndPath("my-bucket", "path/myTopic/0/latest_")
  private val topicPartition = Topic("myTopic").withPartition(0)
  
  "seek" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(false)

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(None)
  }

  "seek" should "return expected offsets for 1 filename when files are not tagged with latest" in {

    when(storageInterface.pathExists(parentBucketAndPath)).thenReturn(true)
    when(storageInterface.list(latestBucketAndPath)).thenReturn(List.empty)
    when(storageInterface.fetchSingleLatestUsingLastModified(parentBucketAndPath, fileNamingStrategy.format)).thenReturn(Some("path/myTopic/0/100.json"))

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/myTopic/0/100.json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))
  }

  "seek" should "return expected offsets for 1 filename when files are tagged with latest" in {

    when(storageInterface.pathExists(parentBucketAndPath)).thenReturn(true)
    when(storageInterface.list(latestBucketAndPath)).thenReturn(List("path/myTopic/0/latest_100.json"))
    when(storageInterface.fetchSingleLatestUsingLastModified(parentBucketAndPath, fileNamingStrategy.format)).thenReturn(None)

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/myTopic/0/latest_100.json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))
  }
  
  "seek" should "clean up old latest offsets for 4 filenames" in {

    when(storageInterface.pathExists(parentBucketAndPath)).thenReturn(true)
    when(storageInterface.list(latestBucketAndPath)).thenReturn(
      List(
        "path/myTopic/0/latest_20.json",
        "path/myTopic/0/latest_40.json",
        "path/myTopic/0/latest_60.json",
        "path/myTopic/0/latest_80.json",
        "path/myTopic/0/latest_100.json"
      )
    )
    when(storageInterface.fetchSingleLatestUsingLastModified(parentBucketAndPath, fileNamingStrategy.format)).thenReturn(None)

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/myTopic/0/latest_100.json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))
    
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/latest_20.json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/20.json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/latest_40.json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/40.json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/latest_60.json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/60.json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/latest_80.json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/myTopic/0/80.json")
    )
  }
}

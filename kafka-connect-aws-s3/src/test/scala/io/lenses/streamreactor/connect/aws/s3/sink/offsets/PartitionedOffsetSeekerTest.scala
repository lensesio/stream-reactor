
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

import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection}
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.PartitionedS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PartitionedOffsetSeekerTest extends AnyFlatSpec with MockitoSugar with Matchers {


  private implicit val fileNamingStrategy = new PartitionedS3FileNamingStrategy(
    FormatSelection(Format.Json),
    PartitionSelection(
      Seq(
        PartitionField("name"),
        PartitionField("title"),
        PartitionField("salary")
      ),
      PartitionDisplay.KeysAndValues
    ))

  private implicit val storageInterface: StorageInterface = mock[StorageInterface]
  
  private val offsetSeeker = new PartitionedOffsetSeeker()

  private val bucketAndPrefix = BucketAndPrefix("my-bucket", Some("path"))
  private val bucketAndPath = BucketAndPath("my-bucket", "path")

  private val topicPartition = Topic("myTopic").withPartition(0)
  
  "seek" should "return empty set when path does not exist" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(false)

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(None)
  }

  "seek" should "return expected offsets for 1 filename when files are not tagged with latest" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(true)
    when(storageInterface.listUsingStringMatching(bucketAndPath, fileNamingStrategy.format)).thenReturn(List.empty)
    when(storageInterface.list(bucketAndPath)).thenReturn(List("path/name=first/title=primary/salary=100/myTopic(0_50).json"))

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/name=first/title=primary/salary=100/myTopic(0_50).json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(50))))
  }
  
  "seek" should "return expected offsets for 1 filename when files are tagged with latest" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(true)
    when(storageInterface.listUsingStringMatching(bucketAndPath, fileNamingStrategy.format)).thenReturn(List("path/name=first/title=primary/salary=100/myTopic(0_latest_50).json"))

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/name=first/title=primary/salary=100/myTopic(0_latest_50).json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(50))))
  }

  "seek" should "clean up old latest offsets for 4 filenames" in {

    when(storageInterface.pathExists(bucketAndPath)).thenReturn(true)
    when(storageInterface.listUsingStringMatching(bucketAndPath, fileNamingStrategy.format)).thenReturn(
      List(
        "path/name=first/title=tertiary/salary=100/myTopic(0_latest_20).json",
        "path/name=second/title=primary/salary=200/myTopic(0_latest_40).json",
        "path/name=first/title=secondary/salary=300/myTopic(0_latest_60).json",
        "path/name=second/title=tertiary/salary=100/myTopic(0_latest_80).json",
        "path/name=first/title=primary/salary=200/myTopic(0_latest_100).json",
      )
    )
    when(storageInterface.list(bucketAndPath)).thenReturn(List.empty)

    offsetSeeker.seek(bucketAndPrefix, topicPartition) should be(Some(BucketAndPath(bucketAndPath.bucket, "path/name=first/title=primary/salary=200/myTopic(0_latest_100).json"), TopicPartitionOffset(Topic("myTopic"), 0, Offset(100))))

    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/name=first/title=tertiary/salary=100/myTopic(0_latest_20).json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/name=first/title=tertiary/salary=100/myTopic(0_20).json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/name=second/title=primary/salary=200/myTopic(0_latest_40).json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/name=second/title=primary/salary=200/myTopic(0_40).json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/name=first/title=secondary/salary=300/myTopic(0_latest_60).json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/name=first/title=secondary/salary=300/myTopic(0_60).json")
    )
    verify(storageInterface).rename(
      BucketAndPath(bucketAndPrefix.bucket, "path/name=second/title=tertiary/salary=100/myTopic(0_latest_80).json"),
      BucketAndPath(bucketAndPrefix.bucket, "path/name=second/title=tertiary/salary=100/myTopic(0_80).json")
    )
  }
}

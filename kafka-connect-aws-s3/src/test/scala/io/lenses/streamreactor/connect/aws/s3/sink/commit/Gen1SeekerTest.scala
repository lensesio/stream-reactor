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

package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPath
import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPrefix
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.sink.HierarchicalS3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.storage.Storage
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Gen1SeekerTest extends AnyWordSpec with MockitoSugar with Matchers {

  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))
  private val bucketAndPath = BucketAndPath("my-bucket", "path/myTopic/0/")

  "Gen1Seeker" should {
    "return empty set when path does not exist" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))

      when(storage.pathExists(bucketAndPath)).thenReturn(false)

      offsetSeeker.latest(Set(TopicPartition(Topic("anothertopic"), 1))) shouldBe Map.empty
    }

    "return expected offsets for 1 filename" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(Vector("path/myTopic/0/100.json"))

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0))) shouldBe
        Map(TopicPartition(Topic("myTopic"), 0) -> Offset(100))
    }

    "return highest offset for multiple offsets of the same file" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(
        Vector(
          "path/myTopic/0/100.json",
          "path/myTopic/0/200.json",
          "path/myTopic/0/300.json")
      )

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0))) shouldBe Map(TopicPartition(Topic("myTopic"), 0) -> Offset(300))
    }

    "return highest offset for multiple offsets of different files" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(
        Vector("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
          "path/notMyTopic/0/300.json", "path/notMyTopic/0/200.json", "path/notMyTopic/0/100.json")
      )

      val bucketAndPathNotMyTopic = BucketAndPath("my-bucket", "path/notMyTopic/0/")
      when(storage.pathExists(bucketAndPathNotMyTopic)).thenReturn(true)
      when(storage.list(bucketAndPathNotMyTopic)).thenReturn(
        Vector("path/myTopic/0/100.json", "path/myTopic/0/200.json", "path/myTopic/0/300.json",
          "path/notMyTopic/0/200.json", "path/notMyTopic/0/100.json")
      )

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0), TopicPartition(Topic("notMyTopic"), 0))) shouldBe
        Map(TopicPartition(Topic("myTopic"), 0) -> Offset(300),
          TopicPartition(Topic("notMyTopic"), 0) -> Offset(200))
    }

    "ignore other file extensions" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(
        Vector(
          "path/myTopic/0/100.avro",
          "path/myTopic/0/200.avro",
          "path/myTopic/0/300.avro",
          "path/myTopic/0/100.json",
          "path/myTopic/0/200.json"
        )
      )

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0))) shouldBe
        Map(TopicPartition(Topic("myTopic"), 0) -> Offset(200))
    }

    "ignore unknown file extensions" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(
        Vector(
          "path/myTopic/0/100.doc",
          "path/myTopic/0/200.xls",
          "path/myTopic/0/300.ppt",
          "path/myTopic/0/100.json",
          "path/myTopic/0/200.json"
        )
      )

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0))) shouldBe
        Map(TopicPartition(Topic("myTopic"), 0) -> Offset(200))
    }

    "ignore files with no extensions" in {
      val storage: Storage = mock[Storage]
      val offsetSeeker = new Gen1Seeker(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("my-bucket", Some("path")))
      when(storage.pathExists(bucketAndPath)).thenReturn(true)
      when(storage.list(bucketAndPath)).thenReturn(
        Vector(
          "path/myTopic/0/100",
          "path/myTopic/0/200",
          "path/myTopic/0/300",
          "path/myTopic/0/100.json",
          "path/myTopic/0/200.json"
        )
      )

      offsetSeeker.latest(Set(TopicPartition(Topic("myTopic"), 0))) shouldBe
        Map(TopicPartition(Topic("myTopic"), 0) -> Offset(200))
    }

  }

}

package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.config.Format.Json
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BucketAndPrefix, Offset, Topic, TopicPartition}
import io.lenses.streamreactor.connect.aws.s3.sink.{HierarchicalS3FileNamingStrategy, PartitionedS3IndexNamingStrategy, S3IndexNamingStrategy}
import io.lenses.streamreactor.connect.aws.s3.storage.Storage
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.ByteArrayInputStream

class Gen2CommitterTest extends AnyWordSpec with MockitoSugar with Matchers {

  private val indexNamingStrategy = new PartitionedS3IndexNamingStrategy
  private val fileNamingStrategy = new HierarchicalS3FileNamingStrategy(FormatSelection(Json))
  private val bucketAndPath = BucketAndPath("bucket", "path/topic/0/")

  "Gen2Seeker" should {

    "return empty map when index does not exist" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))
      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)

      when(storage.pathExists(bucketAndPath)).thenReturn(false)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector.empty)

      offsetSeeker.latest(Set(topicPartition)) shouldBe Map.empty
    }

    "return expected offsets from index" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))

      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector("index/topic/0/latest.22"))

      offsetSeeker.latest(Set(topicPartition)) shouldBe Map(TopicPartition(Topic("topic"), 1) -> Offset(22))
    }

    "return expected offset when obsolete index not removed" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))

      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector("index/topic/0/latest.22", "index/topic/0/latest.35"))
      when(storage.getBlob(BucketAndPath("bucket", "index/topic/0/latest.35"))).thenReturn(new ByteArrayInputStream("path/topic/0/35.json".getBytes))
      when(storage.pathExists(BucketAndPath("bucket", "path/topic/0/35.json"))).thenReturn(true)

      offsetSeeker.latest(Set(topicPartition)) shouldBe Map(TopicPartition(Topic("topic"), 1) -> Offset(35))
    }

    "return expected offsets when latest file not committed" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))

      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector("index/topic/0/latest.22", "index/topic/0/latest.35"))
      when(storage.getBlob(BucketAndPath("bucket", "index/topic/0/latest.35"))).thenReturn(new ByteArrayInputStream("path/topic/0/35.json".getBytes))
      when(storage.pathExists(BucketAndPath("bucket", "path/topic/0/35.json"))).thenReturn(false)

      offsetSeeker.latest(Set(topicPartition)) shouldBe Map(TopicPartition(Topic("topic"), 1) -> Offset(22))
    }

    "fail on more than 2 index files" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))

      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector("index/topic/0/latest.22", "index/topic/0/latest.35", "index/topic/0/latest.48"))

      assertThrows[IllegalStateException] {
        offsetSeeker.latest(Set(topicPartition))
      }
    }

    "ignore non index files" in {
      val storage: Storage = mock[Storage]

      val offsetSeeker = new Gen2Committer(storage, _ => fileNamingStrategy, _ => BucketAndPrefix("bucket", Some("path")))

      val topicPartition = TopicPartition(Topic("topic"), 1)
      val indexBucketAndPath = indexNamingStrategy.indexPath("bucket", topicPartition)
      when(storage.list(indexBucketAndPath)).thenReturn(Vector("index/topic/0/latest.22", "index/topic/0/route.66", "index/topic/0/100.json"))

      offsetSeeker.latest(Set(topicPartition)) shouldBe Map(TopicPartition(Topic("topic"), 1) -> Offset(22))
    }
  }
}

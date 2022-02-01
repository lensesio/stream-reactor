package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, PartitionField, Topic, TopicPartition}
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent.duration.{FiniteDuration, SECONDS}

class S3WriterManagerTest extends AnyFlatSpec with Matchers with S3TestConfig with MockitoSugar {

  private val topicPartition = Topic("topic").withPartition(10)

  "S3WriterManager" should "not throw errors on precommit with no writers and return original OffsetAndMetadata from Kafka Connect" in {
    val wm = new S3WriterManager(
      "myLovelySink",
      _ => DefaultCommitPolicy(Some(5L),Some(FiniteDuration(5, SECONDS)),Some(5L)).asRight,
      _ => RemoteS3RootLocation("bucketAndPath:location").asRight,
      _ => new HierarchicalS3FileNamingStrategy(FormatSelection("csv")).asRight,
      (_,_) => new File("blah.csv").asRight,
      (_,_,_) => RemoteS3PathLocation("bucket", "path").asRight,
      (_,_) => mock[S3FormatWriter].asRight,
    )

    val result = wm.preCommit(Map(topicPartition -> new OffsetAndMetadata(999)))
    result should be (Map(topicPartition -> new OffsetAndMetadata(999)))
  }
}

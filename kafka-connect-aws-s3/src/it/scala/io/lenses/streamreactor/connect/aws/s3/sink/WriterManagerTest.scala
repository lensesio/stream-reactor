package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.commit.FileSize
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Interval
import io.lenses.streamreactor.connect.cloud.common.sink.naming.CloudKeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterIndexer
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent.duration.DurationInt

class WriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with MockitoSugar {
  private implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  private val topicPartition = Topic("topic").withPartition(10)
  private val schemaChangeDetector: SchemaChangeDetector = mock[SchemaChangeDetector]

  private val s3KeyNamer = mock[CloudKeyNamer]
  "S3WriterManager" should "return empty map when no offset or metadata writers can be found" in {
    val wm = new WriterManager(
      commitPolicyFn    = _ => CommitPolicy(FileSize(5L), Interval(5.seconds), Count(5L)).asRight,
      bucketAndPrefixFn = _ => CloudLocation("bucketAndPath:location").asRight,
      keyNamerFn =
        _ => s3KeyNamer.asRight,
      stagingFilenameFn = (_, _) => new File("blah.csv").asRight,
      objKeyBuilderFn   = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn    = (_, _) => mock[FormatWriter].asRight,
      writerIndexer     = mock[WriterIndexer[S3FileMetadata]],
      _.asRight,
      schemaChangeDetector = schemaChangeDetector,
      skipNullValues       = false,
    )

    val result = wm.preCommit(Map(topicPartition -> new OffsetAndMetadata(999)))
    result should be(Map())
  }

}

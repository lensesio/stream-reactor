package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.CsvFormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Count
import io.lenses.streamreactor.connect.aws.s3.sink.commit.FileSize
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Interval
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionSelection.defaultPartitionSelection
import io.lenses.streamreactor.connect.aws.s3.sink.naming.HierarchicalS3FileNamer
import io.lenses.streamreactor.connect.aws.s3.sink.naming.S3KeyNamer
import io.lenses.streamreactor.connect.aws.s3.sink.seek.IndexManager
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.concurrent.duration.DurationInt

class S3WriterManagerTest extends AnyFlatSpec with Matchers with S3ProxyContainerTest with MockitoSugar {
  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sinkName", 1, 1)

  private val topicPartition = Topic("topic").withPartition(10)

  "S3WriterManager" should "return empty map when no offset or metadata writers can be found" in {
    val wm = new S3WriterManager(
      commitPolicyFn    = _ => CommitPolicy(FileSize(5L), Interval(5.seconds), Count(5L)).asRight,
      bucketAndPrefixFn = _ => S3Location("bucketAndPath:location").asRight,
      keyNamerFn =
        _ =>
          new S3KeyNamer(
            CsvFormatSelection(Set.empty),
            NoOpPaddingStrategy.padString,
            defaultPartitionSelection,
            HierarchicalS3FileNamer,
          ).asRight,
      stagingFilenameFn = (_, _) => new File("blah.csv").asRight,
      finalFilenameFn   = (_, _, _) => mock[S3Location].asRight,
      formatWriterFn    = (_, _) => mock[S3FormatWriter].asRight,
      indexManager      = mock[IndexManager],
      _.asRight,
    )

    val result = wm.preCommit(Map(topicPartition -> new OffsetAndMetadata(999)))
    result should be(Map())
  }
}

package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.io.BufferedWriter
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters.MapHasAsJava

class S3SourceJsonEnvelopeTopicFromMetadataTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys
    with TempFileHelper {

  private val MyPrefix = "backups"
  private val TopicA   = "com.olx.posting.olxro.Ad"
  private val TopicB   = "com.olx.posting.olxpl.Ad"

  // Two envelopes pointing at two different original topics, stored in one backup file.
  private val EnvelopeTopicA: String =
    s"""{"key":{"id":"1"},"value":{"id":"1"},"headers":{"h":"v"},"metadata":{"timestamp":1234567890,"topic":"$TopicA","partition":0,"offset":0}}"""
  private val EnvelopeTopicB: String =
    s"""{"key":{"id":"2"},"value":{"id":"2"},"headers":{"h":"v"},"metadata":{"timestamp":1234567891,"topic":"$TopicB","partition":1,"offset":0}}"""

  "task" should "restore each record to the original topic from the envelope metadata" in {
    val existing = listBucketPath(BucketName, s"$MyPrefix/json/")
    if (existing.nonEmpty) storageInterface.deleteFiles(BucketName, existing)
    uploadFile()

    val task = new S3SourceTask()

    val props = (defaultProps ++ Map(
      "connect.s3.kcql" -> s"insert into placeholder select * from $BucketName:$MyPrefix/json STOREAS `JSON` LIMIT 1000 PROPERTIES ('store.envelope'=true, 'source.topic.from.envelope'=true)",
      "connect.s3.source.partition.search.recurse.levels"     -> "0",
      "connect.s3.source.partition.search.continuous"         -> "false",
      SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS                  -> "1000",
    )).asJava

    task.start(props)

    try {
      val sourceRecords = SourceRecordsLoop.loop(task, 10.seconds.toMillis, 2).value
      task.poll() should be(empty)

      val byTopic = sourceRecords.map(r => r.topic() -> r).toMap
      byTopic.keySet shouldBe Set(TopicA, TopicB)
      byTopic(TopicA).kafkaPartition() shouldBe 0
      byTopic(TopicB).kafkaPartition() shouldBe 1
    } finally {
      task.stop()
    }
  }

  private def uploadFile() =
    withFile("00001.json") { file =>
      val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
      bw.write(EnvelopeTopicA)
      bw.newLine()
      bw.write(EnvelopeTopicB)
      bw.flush()
      bw.close()
      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/json/0")
        .leftMap(e => new UploadException(e))
    }
}

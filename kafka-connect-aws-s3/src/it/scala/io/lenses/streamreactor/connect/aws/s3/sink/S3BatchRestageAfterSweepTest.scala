package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData.schema
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import software.amazon.awssdk.services.s3.S3Client

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Armable, test-controlled faults for the IT-only storage seam below.
 *
 * Both faults are persistent while armed: `RetryingStorageInterface` retries `mvFile` (and a
 * one-shot fault would be consumed by the first retry), so a fault fires on every matching call
 * until the test disarms it. `reset()` disarms everything and is called before every test.
 */
final class StorageFaults {
  private val failUploadWhere = new AtomicReference[Option[String => Boolean]](None)
  private val failMoveWhere   = new AtomicReference[Option[String => Boolean]](None)

  /** Fail every `uploadFile` whose destination path ends with `suffix`. */
  def armUploadFailureBySuffix(suffix: String): Unit = failUploadWhere.set(Some(_.endsWith(suffix)))

  /** Fail every `uploadFile` whose destination path satisfies `p`. */
  def armUploadFailureWhere(p: String => Boolean): Unit = failUploadWhere.set(Some(p))

  /** Fail every `mvFile` whose source path satisfies `p`. */
  def armMoveFailureBySource(p: String => Boolean): Unit = failMoveWhere.set(Some(p))

  def disarmUpload(): Unit = failUploadWhere.set(None)
  def disarmMove():   Unit = failMoveWhere.set(None)
  def reset(): Unit = { disarmUpload(); disarmMove() }

  def uploadShouldFail(path: String): Boolean = failUploadWhere.get().exists(_(path))
  def moveShouldFail(source: String): Boolean = failMoveWhere.get().exists(_(source))
}

final class FaultInjectingAwsS3StorageInterface(
  connectorTaskId: ConnectorTaskId,
  s3Client:        S3Client,
  batchDelete:     Boolean,
  faults:          StorageFaults,
) extends AwsS3StorageInterface(connectorTaskId, s3Client, batchDelete, Option.empty) {

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] =
    if (faults.uploadShouldFail(path))
      Left(UploadFailedError(new RuntimeException(s"injected transient upload failure: $bucket/$path"), source.file))
    else super.uploadFile(source, bucket, path)

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] =
    if (faults.moveShouldFail(oldPath))
      Left(
        FileMoveError(new IllegalStateException(s"injected move failure: $oldPath -> $newPath"), oldPath, newPath),
      )
    else super.mvFile(oldBucket, oldPath, newBucket, newPath, maybeEtag)
}

/** IT-only `S3SinkTask` whose storage interface is the fault-injecting one above. */
final class FaultInjectingS3SinkTask(faults: StorageFaults) extends S3SinkTask {
  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          S3SinkConfig,
    cloudClient:     S3Client,
  ): AwsS3StorageInterface =
    new FaultInjectingAwsS3StorageInterface(connectorTaskId, cloudClient, config.batchDelete, faults)
}

/**
 * LC-349: in `exactly.once.commit.mode=batch`, a batch retried after a partial staging failure must
 * not trust a temp object that the `.temp-upload` orphan sweep may have deleted in the meantime.
 */
class S3BatchRestageAfterSweepTest
    extends S3ProxyContainerTest
    with Matchers
    with EitherValues
    with Eventually
    with MockitoSugar
    with BeforeAndAfterEach {

  private val faults = new StorageFaults

  override def beforeEach(): Unit = {
    faults.reset()
    super.beforeEach()
  }

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"
  private val Partition  = 1
  private val kafkaTp    = new TopicPartition(TopicName, Partition)

  private def newTask(): FaultInjectingS3SinkTask = {
    val t = new FaultInjectingS3SinkTask(faults)
    t.initialize(mock[SinkTaskContext])
    t
  }

  private def props: java.util.Map[String, String] =
    (defaultProps ++ Map(
      s"$prefix.kcql" ->
        (s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name STOREAS `JSON` " +
          s"PROPERTIES('${FlushCount.entryName}'=2,'${FlushSize.entryName}'=100000000," +
          s"'${FlushInterval.entryName}'=3600)"),
      s"$prefix.exactly.once.commit.mode"          -> "batch",
      s"$prefix.$ERROR_POLICY_PROP_SUFFIX"         -> "RETRY",
      s"$prefix.indexes.gc.sweep.enabled"          -> "true",
      s"$prefix.indexes.gc.sweep.interval.seconds" -> "1",
      s"$prefix.indexes.gc.sweep.min.age.seconds"  -> "1",
    )).asJava

  private def user(name: String, salary: Double): Struct =
    new Struct(schema).put("name", name).put("title", null).put("salary", salary)

  private def record(name: String, salary: Double, offset: Long): SinkRecord =
    new SinkRecord(TopicName,
                   Partition,
                   null,
                   null,
                   schema,
                   user(name, salary),
                   offset,
                   offset,
                   TimestampType.CREATE_TIME,
    )

  // Seed batch: offsets 0, 1 routed S, S. It commits cleanly and gives the master lock a committed
  // offset. That is a precondition of the scenario, not decoration: the periodic sweep
  // (`IndexManagerV2.sweepAllBatchTemps`) only visits partitions in `seekedOffsets`, which is
  // populated only once the master lock has a committed offset. A partition that has never
  // committed is swept only at `open()`.
  private val seed: List[SinkRecord] =
    List(record("S", 10.0, 0L), record("S", 11.0, 1L))

  // The scenario batch: offsets 2, 3, 4 routed A, B, B. With flush.count=2 the last record makes B
  // flushable, which drives a batch commit of BOTH writers (A with 1 record, B with 2).
  private val records: List[SinkRecord] =
    List(record("A", 0.0, 2L), record("B", 1.0, 3L), record("B", 2.0, 4L))

  private val expectedS = """{"name":"S","title":null,"salary":10.0}{"name":"S","title":null,"salary":11.0}"""
  private val expectedA = """{"name":"A","title":null,"salary":0.0}"""
  private val expectedB = """{"name":"B","title":null,"salary":1.0}{"name":"B","title":null,"salary":2.0}"""

  private def tempKeys(): List[String] = listBucketPath(BucketName, ".temp-upload/")
  private def tempsFor(name: String): List[String] = tempKeys().filter(_.contains(s"/name=$name/"))
  private def finalKeys(): List[String] = listBucketPath(BucketName, s"$PrefixName/")

  private def masterLock(): IndexFile = {
    import IndexFile._
    val locks = listBucketPath(BucketName, ".indexes/").filter(_.endsWith(s"/.locks/Topic($TopicName)/$Partition.lock"))
    locks should have size 1
    storageInterface.getBlobAsObject[IndexFile](BucketName, locks.head).value.wrappedObject
  }

  private val sweepPatience =
    PatienceConfig(timeout = scaled(Span(20, Seconds)), interval = scaled(Span(200, Millis)))

  "batch commit" should "re-stage a swept batch temp on retry and commit every file exactly once (Case 1)" in {
    val task = newTask()
    task.start(props)
    // open() must succeed before relying on the scheduled sweep: it flips acceptingWork on.
    task.open(Seq(kafkaTp).asJava)

    task.put(seed.asJava)
    masterLock().committedOffset shouldBe Some(Offset(1))
    finalKeys().filter(_.contains("/name=S/")) should have size 1

    // Attempt 1: A stages fine, B's stage upload fails transiently -> RetriableException under RETRY.
    faults.armUploadFailureWhere(p => p.startsWith(".temp-upload/") && p.contains("/name=B/"))
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    tempsFor("A") should have size 1
    tempsFor("B") shouldBe empty
    finalKeys().filterNot(_.contains("/name=S/")) shouldBe empty

    // Nothing references A's temp (no CAS happened), so the periodic sweep (min age 1 s) reaps it.
    eventually {
      tempsFor("A") shouldBe empty
    }(sweepPatience, implicitly, implicitly)

    // Attempt 2: storage is healthy again; Connect re-delivers the same records.
    faults.disarmUpload()
    Try(task.put(records.asJava)) match {
      case Success(_)        =>
      case Failure(retryErr) =>
        // Red-path diagnostics: show what the retry persisted, and that a fresh task cannot open.
        val _ = Try(task.stop())
        val reopen = Try {
          val fresh = newTask()
          fresh.start(props)
          try fresh.open(Seq(kafkaTp).asJava)
          finally { val _ = Try(fresh.stop()) }
        }
        val pending = Try(masterLock().pendingState)
        fail(
          s"retry put() failed with ${retryErr.getClass.getName}: ${retryErr.getMessage}; " +
            s"persisted PendingState=$pending; fresh task open() => $reopen",
          retryErr,
        )
    }

    // Both final files exist exactly once, with the exact records (plus the untouched seed file).
    val finals = finalKeys()
    finals should have size 3
    val sFiles = finals.filter(_.contains("/name=S/"))
    sFiles should have size 1
    remoteFileAsString(BucketName, sFiles.head) shouldBe expectedS
    val aFiles = finals.filter(_.contains("/name=A/"))
    val bFiles = finals.filter(_.contains("/name=B/"))
    aFiles should have size 1
    bFiles should have size 1
    remoteFileAsString(BucketName, aFiles.head) shouldBe expectedA
    remoteFileAsString(BucketName, bFiles.head) shouldBe expectedB

    // None of the retry's temps remain (they were moved), and the swept original is not back.
    tempKeys() shouldBe empty

    // The master lock carries no PendingState and is at the batch offset.
    val lock = masterLock()
    lock.pendingState shouldBe None
    lock.committedOffset shouldBe Some(Offset(4))

    // preCommit reports the next offset to consume.
    val committed = task.preCommit(Map(kafkaTp -> new OffsetAndMetadata(5L)).asJava).asScala
    committed.get(kafkaTp).map(_.offset()) shouldBe Some(5L)

    task.close(Seq(kafkaTp).asJava)
    task.stop()
  }
}

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.common.config.base.const.TraitConfigConst.ERROR_POLICY_PROP_SUFFIX
import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
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
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.HeadObjectRequest

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.CollectionHasAsScala
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
  private val uploads         = new ConcurrentLinkedQueue[String]()

  /** Fail every `uploadFile` whose destination path ends with `suffix`. */
  def armUploadFailureBySuffix(suffix: String): Unit = failUploadWhere.set(Some(_.endsWith(suffix)))

  /** Fail every `uploadFile` whose destination path satisfies `p`. */
  def armUploadFailureWhere(p: String => Boolean): Unit = failUploadWhere.set(Some(p))

  /** Fail every `mvFile` whose source path satisfies `p`. */
  def armMoveFailureBySource(p: String => Boolean): Unit = failMoveWhere.set(Some(p))

  def disarmUpload(): Unit = failUploadWhere.set(None)
  def disarmMove():   Unit = failMoveWhere.set(None)
  def reset(): Unit = { disarmUpload(); disarmMove(); uploads.clear() }

  /** Records a successful `uploadFile` (data files only: lock writes do not go through it). */
  def recordUpload(path: String): Unit = { val _ = uploads.add(path) }

  /** Every path successfully uploaded via `uploadFile` since the last `reset()`. */
  def uploadedPaths: List[String] = uploads.asScala.toList

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
    else {
      val result = super.uploadFile(source, bucket, path)
      result.foreach(_ => faults.recordUpload(path))
      result
    }

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

  // ---------------------------------------------------------------------------------------------
  // Restart paths after a re-staged batch commit (LC-349 AC3a, AC3b). Both start like Case 1: a
  // committed seed batch, a partial stage failure, and the periodic sweep reaping A's unreferenced
  // temp. The retry then re-stages and CASes, and a copy fails after the CAS (Fatal). A fresh task
  // must resume the chain from the persisted PendingState.
  // ---------------------------------------------------------------------------------------------

  /** Seed batch committed, then attempt 1 fails staging B, then the sweep reaps A's temp. */
  private def seedThenPartialStageThenSweep(task: FaultInjectingS3SinkTask): Unit = {
    task.put(seed.asJava)
    masterLock().committedOffset shouldBe Some(Offset(1))

    faults.armUploadFailureWhere(p => p.startsWith(".temp-upload/") && p.contains("/name=B/"))
    intercept[RetriableException] {
      task.put(records.asJava)
    }
    tempsFor("A") should have size 1
    tempsFor("B") shouldBe empty
    eventually {
      tempsFor("A") shouldBe empty
    }(sweepPatience, implicitly, implicitly)
    faults.disarmUpload()
  }

  private def copySources(lock: IndexFile): List[String] =
    lock.pendingState.toList.flatMap(_.pendingOperations.toList).collect { case c: CopyOperation => c.source }

  private def exists(key: String): Boolean = listBucketPath(BucketName, key).contains(key)

  /**
   * Proves at least `passes` periodic sweep passes ran, without sleeping: plants an aged-out,
   * unreferenced sentinel temp under the partition's batch temp prefix and waits until the sweep
   * reaps it, `passes` times in a row.
   */
  private def awaitSweepPasses(passes: Int, partitionTempPrefix: String): Unit =
    (1 to passes).foreach { n =>
      val sentinel = s"${partitionTempPrefix}sentinel-$n/marker.json"
      storageInterface.writeStringToFile(BucketName, sentinel, UploadableString("sentinel")).value
      exists(sentinel) shouldBe true
      eventually {
        exists(sentinel) shouldBe false
      }(sweepPatience, implicitly, implicitly)
    }

  /** `.temp-upload/<connector>/<topic>/<partition>/` taken from an existing batch temp key. */
  private def partitionTempPrefixOf(tempKey: String): String =
    tempKey.split('/').take(4).mkString("", "/", "/")

  private def eTagOf(key: String): String =
    client.headObject(HeadObjectRequest.builder().bucket(BucketName).key(key).build()).eTag()

  private def assertBatchLandedOnce(): Assertion = {
    val finals = finalKeys()
    finals should have size 3
    val sFiles = finals.filter(_.contains("/name=S/"))
    val aFiles = finals.filter(_.contains("/name=A/"))
    val bFiles = finals.filter(_.contains("/name=B/"))
    sFiles should have size 1
    aFiles should have size 1
    bFiles should have size 1
    remoteFileAsString(BucketName, sFiles.head) shouldBe expectedS
    remoteFileAsString(BucketName, aFiles.head) shouldBe expectedA
    remoteFileAsString(BucketName, bFiles.head) shouldBe expectedB
    tempKeys() shouldBe empty
    val lock = masterLock()
    lock.pendingState shouldBe None
    lock.committedOffset shouldBe Some(Offset(4))
  }

  /** Starts and opens a fresh task; on failure, reports the persisted state for diagnosis. */
  private def restartedTask(): FaultInjectingS3SinkTask = {
    val fresh = newTask()
    fresh.start(props)
    Try(fresh.open(Seq(kafkaTp).asJava)) match {
      case Success(_) => fresh
      case Failure(openErr) =>
        val pending = Try(masterLock().pendingState)
        val sources = Try(copySources(masterLock()).map(s => s -> exists(s)))
        val _       = Try(fresh.stop())
        fail(
          s"fresh task open() failed with ${openErr.getClass.getName}: ${openErr.getMessage}; " +
            s"persisted PendingState=$pending; copy sources (exists?)=$sources",
          openErr,
        )
    }
  }

  it should "survive a failed copy of A after the retry's CAS and resume on restart (Case 2a)" in {
    val task = newTask()
    task.start(props)
    task.open(Seq(kafkaTp).asJava)
    seedThenPartialStageThenSweep(task)

    // Attempt 2: the retry re-stages and CASes; the copy of A (the chain's first op) fails.
    faults.armMoveFailureBySource(p => p.startsWith(".temp-upload/") && p.contains("/name=A/"))
    val retryErr = intercept[Throwable] {
      task.put(records.asJava)
    }
    retryErr shouldBe a[FatalConnectException]

    // Past the CAS: the lock records both copies and nothing landed.
    val pendingAfterFail = masterLock()
    pendingAfterFail.committedOffset shouldBe Some(Offset(1))
    val sources = copySources(pendingAfterFail)
    sources should have size 2
    finalKeys().filterNot(_.contains("/name=S/")) shouldBe empty
    // Every persisted copy source exists right after the failure. On unfixed code (no re-stage),
    // A's source is the temp the sweep already deleted.
    withClue(s"persisted copy sources (exists?)=${sources.map(s => s -> exists(s))}: ") {
      sources.forall(exists) shouldBe true
    }

    // The referenced temps survive at least two further periodic sweep passes.
    awaitSweepPasses(2, partitionTempPrefixOf(sources.head))
    sources.foreach(s => withClue(s"referenced temp $s after two sweep passes: ")(exists(s) shouldBe true))

    // Stop, disarm, and restart: a fresh task's open() resumes the chain.
    Try(task.close(Seq(kafkaTp).asJava))
    task.stop()
    faults.disarmMove()
    val uploadsBeforeRestart = faults.uploadedPaths

    val fresh = restartedTask()
    try {
      assertBatchLandedOnce()
      faults.uploadedPaths shouldBe uploadsBeforeRestart

      // Connect re-delivers the batch (it was never acknowledged): deduplicated, no extra files.
      fresh.put(records.asJava)
      fresh.put(java.util.Collections.emptyList[SinkRecord]())
      assertBatchLandedOnce()
      faults.uploadedPaths shouldBe uploadsBeforeRestart

      val committed = fresh.preCommit(Map(kafkaTp -> new OffsetAndMetadata(5L)).asJava).asScala
      committed.get(kafkaTp).map(_.offset()) shouldBe Some(5L)
    } finally {
      fresh.close(Seq(kafkaTp).asJava)
      fresh.stop()
    }
  }

  it should "land B on restart after A landed, without rewriting A or re-uploading (Case 2b)" in {
    val task = newTask()
    task.start(props)
    task.open(Seq(kafkaTp).asJava)
    seedThenPartialStageThenSweep(task)

    // Attempt 2: the retry re-stages and CASes; A's copy lands, then B's copy fails.
    faults.armMoveFailureBySource(p => p.startsWith(".temp-upload/") && p.contains("/name=B/"))
    val retryErr = intercept[Throwable] {
      task.put(records.asJava)
    }
    retryErr shouldBe a[FatalConnectException]

    val aFiles = finalKeys().filter(_.contains("/name=A/"))
    aFiles should have size 1
    remoteFileAsString(BucketName, aFiles.head) shouldBe expectedA
    val aETag = eTagOf(aFiles.head)
    finalKeys().filter(_.contains("/name=B/")) shouldBe empty
    // The chain recorded A's copy: only B's remains.
    val remaining = copySources(masterLock())
    remaining should have size 1
    remaining.head should include("/name=B/")
    exists(remaining.head) shouldBe true

    Try(task.close(Seq(kafkaTp).asJava))
    task.stop()
    faults.disarmMove()
    val uploadsBeforeRestart = faults.uploadedPaths

    val fresh = restartedTask()
    try {
      assertBatchLandedOnce()
      eTagOf(aFiles.head) shouldBe aETag
      faults.uploadedPaths shouldBe uploadsBeforeRestart

      fresh.put(records.asJava)
      fresh.put(java.util.Collections.emptyList[SinkRecord]())
      assertBatchLandedOnce()
      eTagOf(aFiles.head) shouldBe aETag
      faults.uploadedPaths shouldBe uploadsBeforeRestart
    } finally {
      fresh.close(Seq(kafkaTp).asJava)
      fresh.stop()
    }
  }
}

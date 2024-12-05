package io.lenses.streamreactor.connect.http.sink
import cats.data.NonEmptySeq
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.RecordsQueueBatcher.takeBatch
import io.lenses.streamreactor.connect.http.sink.commit.BatchPolicy
import io.lenses.streamreactor.connect.http.sink.commit.BatchResult
import io.lenses.streamreactor.connect.http.sink.commit.Count
import io.lenses.streamreactor.connect.http.sink.commit.FileSize
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.commit.Interval
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import scala.collection.immutable.Queue

class RecordsQueueBatcherTest extends AnyFunSuiteLike with MockitoSugar with Matchers with LazyLogging {

  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  private val topicPartition: TopicPartition = Topic("myTopic").withPartition(1)

  private val testEndpoint = "https://mytestendpoint.example.com"

  private val timestamp1 = 100L
  private val record1    = RenderedRecord(topicPartition.atOffset(100), timestamp1, "record1", Seq.empty, testEndpoint)

  private val timestamp2 = 200L
  private val record2    = RenderedRecord(topicPartition.atOffset(101), timestamp2, "record2", Seq.empty, testEndpoint)

  private val timestamp3 = 300L
  private val record3    = RenderedRecord(topicPartition.atOffset(102), timestamp3, "record3", Seq.empty, testEndpoint)

  private val records = Queue(record1, record2, record3)

  test("takeBatch should return whole batch when all records require flush") {
    val batchPolicy = mock[BatchPolicy]
    when(batchPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(
      BatchResult(fitsInBatch = true, triggerReached = false, greedyTriggerReached = false),
      BatchResult(fitsInBatch = true, triggerReached = false, greedyTriggerReached = false),
      BatchResult(fitsInBatch = true, triggerReached = true, greedyTriggerReached  = false),
    )

    val result = takeBatch(batchPolicy, defaultContext, records)

    result match {
      case NonEmptyBatchInfo(batch, updatedCommitContext, queueSize) =>
        batch should be(NonEmptySeq.of(record1, record2, record3))
        // TODO
        updatedCommitContext should not be defaultContext
        queueSize should be(3)
      case _ => fail("Should be a non-empty batch")
    }
  }

  test("takeBatch should return empty batch when no records require flush") {
    val batchPolicy = mock[BatchPolicy]
    when(batchPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(BatchResult(false, false, false))

    val result = takeBatch(batchPolicy, defaultContext, records)
    result match {
      case EmptyBatchInfo(queueSize) =>
        queueSize should be(3)
      case _ => fail("Should be an empty batch")
    }
  }

  test("takeBatch should return batch with partial records when commit policy requires flush mid-way") {
    val batchPolicy = mock[BatchPolicy]
    when(batchPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(
      BatchResult(fitsInBatch = true, triggerReached = false, false),
      BatchResult(fitsInBatch = true, triggerReached = true, false),
    )

    val result = takeBatch(batchPolicy, defaultContext, records)

    result match {
      case NonEmptyBatchInfo(batch, updatedCommitContext, queueSize) =>
        batch should be(NonEmptySeq.of(record1, record2))
        // TODO
        updatedCommitContext should not be defaultContext
        queueSize should be(3)
      case _ => fail("Should be a non-empty batch")
    }

  }

  test("takeBatch should return empty batch when the queue is empty") {
    val batchPolicy = mock[BatchPolicy]
    when(batchPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(BatchResult(true, true, false))

    val result = takeBatch(batchPolicy, defaultContext, Queue.empty[RenderedRecord])
    result match {
      case EmptyBatchInfo(queueSize) =>
        queueSize should be(0)
      case _ => fail("Should be an empty batch")
    }
  }

  test("takeBatch should return whole batch when there is only one record and it requires flush") {
    val batchPolicy = mock[BatchPolicy]
    when(batchPolicy.shouldBatch(any[HttpCommitContext])).thenReturn(BatchResult(fitsInBatch          = true,
                                                                                 triggerReached       = true,
                                                                                 greedyTriggerReached = false,
    ))
    val commitPolicy = mock[CommitPolicy]
    when(commitPolicy.shouldFlush(any[HttpCommitContext])).thenReturn(true)

    val singleRecordQueue = Queue(record1)
    val result            = takeBatch(batchPolicy, defaultContext, singleRecordQueue)

    result match {
      case NonEmptyBatchInfo(batch, updatedCommitContext, queueSize) =>
        batch should be(NonEmptySeq.of(record1))
        updatedCommitContext should not be defaultContext
        queueSize should be(1)
      case _ => fail("Should be a non-empty batch")
    }
  }

  val startTime = System.currentTimeMillis() - 3000
  private val commitPolicies = Table(
    ("description", "startTime", "fileSize", "interval", "count", "expectedBatchSize", "expectedQueueSize"),
    ("commit policy - interval trigger + count trigger (1)", startTime, 1_000_000L, Duration.ofSeconds(1), 1L, 1, 3),
    ("commit policy - interval trigger + count trigger (2)", startTime, 1_000_000L, Duration.ofSeconds(1), 2L, 2, 3),
    ("commit policy - interval trigger + count trigger (3)", startTime, 1_000_000L, Duration.ofSeconds(1), 3L, 3, 3),
    ("commit policy - no triggers should not batch (0)", startTime, 1_000_000L, Duration.ofMinutes(10), 30L, 0, 3),
    ("commit policy - interval trigger only", startTime, 1_000_000L, Duration.ofSeconds(1), 30L, 3, 3),
    ("commit policy - file size trigger only", startTime, 14L, Duration.ofMinutes(10), 30L, 2, 3),
    ("commit policy - count trigger only (1)", startTime, 1_000_000L, Duration.ofMinutes(10), 1L, 1, 3),
    ("commit policy - count trigger only (2)", startTime, 1_000_000L, Duration.ofMinutes(10), 2L, 2, 3),
    ("commit policy - count trigger only (3)", startTime, 1_000_000L, Duration.ofMinutes(10), 3L, 3, 3),
    ("commit policy - count + file size trigger (1)", startTime, 1_000_000L, Duration.ofMinutes(10), 1L, 1, 3),
    ("commit policy - count + file size trigger (2)", startTime, 1_000_000L, Duration.ofMinutes(10), 2L, 2, 3),
    ("commit policy - count + file size trigger (3)", startTime, 1_000_000L, Duration.ofMinutes(10), 3L, 3, 3),
    ("commit policy - interval + file size trigger (1)", startTime, 1_000_000L, Duration.ofSeconds(1), 30L, 3, 3),
    ("commit policy - interval + file size trigger (2)", startTime, 1_000_000L, Duration.ofSeconds(1), 30L, 3, 3),
    ("commit policy - interval + file size trigger (3)", startTime, 1_000_000L, Duration.ofSeconds(1), 30L, 3, 3),
  )

  forAll(commitPolicies) {
    (description, startTime, fileSize, interval, count, expectedBatchSize, expectedQueueSize) =>
      test(s"takeBatch should work with $description") {
        logger.info(s"test - $description")
        val clock = new Clock {
          override def getZone: ZoneId = ZoneId.systemDefault()

          override def instant(): Instant = Instant.ofEpochMilli(startTime + 3000)

          override def withZone(zone: ZoneId): Clock = fail("No")
        }
        val batchPolicy = new BatchPolicy(
          logger,
          FileSize(fileSize),
          Count(count),
          Interval(interval, clock),
        )

        val recordQueue = Queue(record1, record2, record3)
        val result = takeBatch(batchPolicy,
                               defaultContext.copy(
                                 createdTimestamp     = startTime,
                                 lastFlushedTimestamp = startTime.some,
                               ),
                               recordQueue,
        )

        result match {
          case EmptyBatchInfo(queueSize) if expectedBatchSize == 0 =>
            queueSize should be(expectedQueueSize)
          case NonEmptyBatchInfo(batch, updatedCommitContext, queueSize) if expectedBatchSize > 0 =>
            batch.length should be(expectedBatchSize)
            updatedCommitContext should not be defaultContext
            queueSize should be(expectedQueueSize)
            batch.toSeq should be(recordQueue.slice(0, expectedBatchSize))

          case other =>
            fail(
              s"Unexpected result for $description, expectedBatchSize: $expectedBatchSize, batchSize: expectedQueueSize: $expectedQueueSize, was: ${other.getClass.getSimpleName} ${other.queueSize}",
            )
        }
      }

  }

}

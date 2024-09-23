/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink

import cats.effect.IO
import cats.effect.Ref
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import cyclops.data.tuple
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.updateCommitContextPostCommit
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.model.generic.ReportingRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata

import java.util
import scala.collection.immutable.Queue
import scala.jdk.CollectionConverters.SeqHasAsJava

class HttpWriter(
  sinkName:         String,
  commitPolicy:     CommitPolicy,
  sender:           HttpRequestSender,
  template:         TemplateType,
  recordsQueueRef:  Ref[IO, Queue[RenderedRecord]],
  commitContextRef: Ref[IO, HttpCommitContext],
  errorThreshold:   Int,
  tidyJson:         Boolean,
  errorReporter:    ReportingController,
  successReporter:  ReportingController,
) extends LazyLogging {
  private val maybeBatchSize: Option[Int] = commitPolicy.conditions.collectFirst {
    case Count(maxCount) => maxCount.toInt
  }

  // TODO: feedback to kafka a warning if the queue gets too large

  // adds records to the queue.  Returns immediately - processing occurs asynchronously.
  def add(newRecords: Seq[RenderedRecord]): IO[Unit] =
    recordsQueueRef.modify { currentQueue =>
      val updatedQueue = currentQueue.enqueueAll(newRecords)
      (updatedQueue, ())
    }

  // called on a loop to process the queue
  def process(): IO[Unit] = {
    for {
      _ <- IO(
        logger.debug(s"[$sinkName] HttpWriter.process, queue size: ${recordsQueueRef.get.map(_.size).unsafeRunSync()}"),
      )
      recordQueue <- recordsQueueRef.get
      res <- recordQueue match {
        case recordsQueue: Queue[RenderedRecord] if recordsQueue.nonEmpty =>
          for {
            _          <- IO(logger.debug(s"[$sinkName] Queue is not empty"))
            takeHowMany = maybeBatchSize.getOrElse(recordsQueue.size)
            _          <- IO(logger.debug(s"[$sinkName] Required batch size is $takeHowMany"))

            batch: Queue[RenderedRecord] <- IO(recordsQueue.take(takeHowMany))
            _      <- IO(logger.info(s"[$sinkName] Batch of ${batch.size}"))
            _      <- modifyCommitContext(batch)
            refSet <- recordsQueueRef.set(dequeueN(recordsQueue, takeHowMany))
          } yield refSet
        case _ =>
          IO(logger.trace(s"[$sinkName] Empty record queue"))
      }
      _ <- resetErrorsInCommitContext()
    } yield res
  }.onError {
    e =>
      for {
        uniqueError: Option[Throwable] <- addErrorToCommitContext(e)
        res <- if (uniqueError.nonEmpty) {
          IO(logger.error("Error in HttpWriter", e)) *> IO.raiseError(e)
        } else {
          IO(logger.error("Error in HttpWriter but not reached threshold so ignoring", e)) *> IO.unit
        }
      } yield res
  }

  def preCommit(
    initialOffsetAndMetaMap: Map[TopicPartition, OffsetAndMetadata],
  ): IO[Map[TopicPartition, OffsetAndMetadata]] =
    commitContextRef.get.map {
      case HttpCommitContext(_, committedOffsets, _, _, _, _, _) =>
        committedOffsets.flatMap {
          case (tp, offset) =>
            for {
              initialOffsetAndMeta <- initialOffsetAndMetaMap.get(tp)

            } yield tp -> new OffsetAndMetadata(offset.value,
                                                initialOffsetAndMeta.leaderEpoch(),
                                                initialOffsetAndMeta.metadata(),
            )
        }
      case _ => initialOffsetAndMetaMap
    }.orElse(IO(Map.empty[TopicPartition, OffsetAndMetadata]))

  private def addErrorToCommitContext(e: Throwable): IO[Option[Throwable]] = {
    val updatedCC = commitContextRef.getAndUpdate {
      commitContext => commitContext.addError(e)
    }
    val maxError = updatedCC.map(cc =>
      cc
        .errors
        .maxByOption { case (_, errSeq) => errSeq.size }
        .filter { case (_, errSeq) => errSeq.size > errorThreshold }
        .flatMap(_._2.headOption),
    )
    maxError
  }

  private def resetErrorsInCommitContext(): IO[Unit] =
    commitContextRef.getAndUpdate {
      commitContext => commitContext.resetErrors
    } *> IO.unit

  private def updateCommitContextIfFlush(
    cc:    HttpCommitContext,
    batch: Queue[RenderedRecord],
  ): IO[HttpCommitContext] =
    for {
      flushEvalCommitContext: HttpCommitContext <- IO.pure(createCommitContextForEvaluation(batch, cc))
      _ <- IO.delay(logger.trace(s"[$sinkName] Updating sink context to: $flushEvalCommitContext"))
      shouldFlush: Boolean <- IO.pure(commitPolicy.shouldFlush(flushEvalCommitContext))
      _ <- IO.delay(logger.trace(s"[$sinkName] Should flush? $shouldFlush"))
      _ <- if (shouldFlush) {
        for {
          _                 <- IO.delay(logger.trace(s"[$sinkName] Flushing batch"))
          processedTemplate <- flush(batch)
        } yield processedTemplate
      } else {
        IO.unit
      }
    } yield {
      if (shouldFlush) {
        updateCommitContextPostCommit(currentCommitContext = flushEvalCommitContext)
      } else {
        cc
      }
    }

  private def modifyCommitContext(batch: Queue[RenderedRecord]): IO[Unit] = {
    logger.trace(s"[$sinkName] modifyCommitContext for batch of ${batch.size}")

    commitContextRef.modify {
      cc: HttpCommitContext =>
        updateCommitContextIfFlush(cc, batch)
          .map((_, ())).unsafeRunSync()
    }
  }

  private def dequeueN[A](rQ: Queue[A], n: Int): Queue[A] =
    rQ.splitAt(n) match {
      case (_, remaining) => remaining
    }

  private def flush(records: Seq[RenderedRecord]): IO[ProcessedTemplate] =
    for {
      processed <- IO.fromEither(template.process(records, tidyJson))
      _         <- reportResult(records, processed, sender.sendHttpRequest(processed))
    } yield processed

  private def reportResult(
    renderedRecords:   Seq[RenderedRecord],
    processedTemplate: ProcessedTemplate,
    responseIo:        IO[Unit],
  ): IO[Unit] = {
    val maxRecord = OffsetMergeUtils.maxRecord(renderedRecords)

    val reportRecord = (templateContent: String) =>
      new ReportingRecord(
        maxRecord.topicPartitionOffset.toTopicPartition.toKafka,
        maxRecord.topicPartitionOffset.offset.value,
        maxRecord.timestamp,
        processedTemplate.endpoint,
        templateContent,
        convertToCyclopsTuples(processedTemplate.headers),
      )

    responseIo.flatTap { _ =>
      IO(successReporter.enqueue(reportRecord(processedTemplate.content)))
    }.handleErrorWith { error =>
      IO(errorReporter.enqueue(reportRecord(error.getMessage))) *> IO.raiseError(error)
    }
  }

  private def convertToCyclopsTuples(headers: Seq[(String, String)]): util.List[tuple.Tuple2[String, String]] =
    headers.map {
      case (hk, hv) => new tuple.Tuple2(hk, hv)
    }.asJava

}

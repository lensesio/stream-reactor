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
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToCyclopsOption
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.updateCommitContextPostCommit
import io.lenses.streamreactor.connect.http.sink.client.HttpRequestSender
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseFailure
import io.lenses.streamreactor.connect.http.sink.client.HttpResponseSuccess
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import io.lenses.streamreactor.connect.http.sink.tpl.TemplateType
import io.lenses.streamreactor.connect.reporting.ReportingController
import io.lenses.streamreactor.connect.reporting.model.ConnectorSpecificRecordData
import io.lenses.streamreactor.connect.reporting.model.ReportingRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata

import scala.collection.mutable

class HttpWriter(
  sinkName:         String,
  commitPolicy:     CommitPolicy,
  sender:           HttpRequestSender,
  template:         TemplateType,
  recordsQueue:     mutable.Queue[RenderedRecord],
  commitContextRef: Ref[IO, HttpCommitContext],
  errorThreshold:   Int,
  tidyJson:         Boolean,
  errorReporter:    ReportingController[HttpFailureConnectorSpecificRecordData],
  successReporter:  ReportingController[HttpSuccessConnectorSpecificRecordData],
  queueLock:        Semaphore[IO],
) extends LazyLogging {
  private val maybeBatchSize: Option[Int] = commitPolicy.conditions.collectFirst {
    case Count(maxCount) => maxCount.toInt
  }

  // TODO: feedback to kafka a warning if the queue gets too large

  // adds records to the queue.  Returns immediately - processing occurs asynchronously.
  def add(newRecords: Seq[RenderedRecord]): IO[Unit] =
    queueLock.permit.use { _ =>
      IO(recordsQueue.enqueueAll(newRecords)).void // Discard the result of enqueueAll
    }

  // called on a loop to process the queue
  def process(): IO[Unit] =
    queueLock.permit.use { _ =>
      for {
        _           <- IO(logger.debug(s"[$sinkName] HttpWriter.process, queue size: ${recordsQueue.size}"))
        takeHowMany <- IO.pure(maybeBatchSize.getOrElse(recordsQueue.size))
        batch: mutable.Queue[RenderedRecord] <- IO(recordsQueue.take(takeHowMany))
        _               <- IO(logger.info(s"[$sinkName] Batch of ${batch.size} / $takeHowMany"))
        _               <- modifyCommitContext(batch)
        removedElements <- IO(recordsQueue.removeHeadWhile(batch.contains(_)))
        _               <- resetErrorsInCommitContext()

      } yield removedElements

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
    } *> IO.unit

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
    batch: mutable.Queue[RenderedRecord],
  ): IO[HttpCommitContext] =
    for {
      flushEvalCommitContext: HttpCommitContext <- IO.pure(createCommitContextForEvaluation(batch.toSeq, cc))
      _ <- IO.delay(logger.trace(s"[$sinkName] Updating sink context to: $flushEvalCommitContext"))
      shouldFlush: Boolean <- IO.pure(commitPolicy.shouldFlush(flushEvalCommitContext))
      _ <- IO.delay(logger.trace(s"[$sinkName] Should flush? $shouldFlush"))
      _ <- if (shouldFlush) {
        for {
          _                 <- IO.delay(logger.trace(s"[$sinkName] Flushing batch"))
          processedTemplate <- flush(batch.toSeq)
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

  private def modifyCommitContext(batch: mutable.Queue[RenderedRecord]): IO[Unit] = {
    logger.trace(s"[$sinkName] modifyCommitContext for batch of ${batch.size}")

    commitContextRef.modify {
      cc: HttpCommitContext =>
        updateCommitContextIfFlush(cc, batch)
          .map((_, ())).unsafeRunSync()
    }
  }

  private def flush(records: Seq[RenderedRecord]): IO[ProcessedTemplate] =
    for {
      processed <- IO.fromEither(template.process(records, tidyJson))
      _         <- reportResult(records, processed, sender.sendHttpRequest(processed))
    } yield processed

  private def reportResult(
    renderedRecords:   Seq[RenderedRecord],
    processedTemplate: ProcessedTemplate,
    responseIo:        IO[Either[HttpResponseFailure, HttpResponseSuccess]],
  ): IO[Unit] = {
    val maxRecord = OffsetMergeUtils.maxRecord(renderedRecords)

    def reportRecord[C <: ConnectorSpecificRecordData]: C => ReportingRecord[C] = (connectorSpecific: C) =>
      new ReportingRecord[C](
        maxRecord.topicPartitionOffset.toTopicPartition.toKafka,
        maxRecord.topicPartitionOffset.offset.value,
        maxRecord.timestamp,
        processedTemplate.endpoint,
        processedTemplate.content,
        connectorSpecific,
      )

    responseIo.flatMap {
      case Left(error) => IO(
          errorReporter.enqueue(
            reportRecord[HttpFailureConnectorSpecificRecordData](
              HttpFailureConnectorSpecificRecordData(
                convertToCyclopsOption(error.statusCode).map(_.toInt),
                convertToCyclopsOption(error.responseContent),
                error.getMessage,
              ),
            ),
          ),
        ) *> IO.raiseError(error)
      case Right(success) => IO(
          successReporter.enqueue(
            reportRecord[HttpSuccessConnectorSpecificRecordData](
              HttpSuccessConnectorSpecificRecordData(
                success.statusCode,
                convertToCyclopsOption(success.responseContent),
              ),
            ),
          ),
        )
    }
  }

}

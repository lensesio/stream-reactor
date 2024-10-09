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

class HttpWriter(
  sinkName:         String,
  sender:           HttpRequestSender,
  template:         TemplateType,
  recordsQueue:     RecordsQueue,
  commitContextRef: Ref[IO, HttpCommitContext],
  errorThreshold:   Int,
  tidyJson:         Boolean,
  errorReporter:    ReportingController[HttpFailureConnectorSpecificRecordData],
  successReporter:  ReportingController[HttpSuccessConnectorSpecificRecordData],
  queueLock:        Semaphore[IO],
) extends LazyLogging {

  // TODO: feedback to kafka a warning if the queue gets too large

  // adds records to the queue.  Returns immediately - processing occurs asynchronously.
  def add(newRecords: Seq[RenderedRecord]): IO[Unit] =
    queueLock.permit.use { _ =>
      IO(recordsQueue.enqueueAll(newRecords))
    }

  private case class NoBatchYetError(queueSize: Int) extends Exception

  // called on a loop to process the queue
  def process(): IO[Unit] =
    queueLock.permit.use { _ =>
      for {
        batchInfo <- IO(recordsQueue.takeBatch())
        nonEmptyBatchInfo <- batchInfo match {
          case EmptyBatchInfo(totalQueueSize) => IO.raiseError(NoBatchYetError(totalQueueSize))
          case nonEmptyBatchInfo @ NonEmptyBatchInfo(batch, _, totalQueueSize) => IO(
              logger.debug(
                s"[$sinkName] HttpWriter.process, batch of ${batch.length}, queue size: $totalQueueSize",
              ),
            )
              .map(_ => nonEmptyBatchInfo)
        }
        _               <- modifyCommitContext(nonEmptyBatchInfo)
        removedElements <- IO(recordsQueue.dequeue(nonEmptyBatchInfo.batch))
        _               <- resetErrorsInCommitContext()

      } yield removedElements

    }.handleErrorWith {
      case _: NoBatchYetError => IO.unit
      case e =>
        for {
          uniqueError: Option[Throwable] <- addErrorToCommitContext(e)
          _ <- if (uniqueError.nonEmpty) {
            IO(logger.error("Error in HttpWriter", e)) *> IO.raiseError(e)
          } else {
            IO(logger.error("Error in HttpWriter but not reached threshold so ignoring", e)) *> IO.unit
          }
        } yield IO.unit
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

  private def modifyCommitContext(batch: NonEmptyBatchInfo): IO[Unit] = {
    logger.trace(s"[$sinkName] modifyCommitContext for batch of ${batch.batch.length}")

    commitContextRef.modify {
      cc: HttpCommitContext =>
        (for {
          _ <- IO.delay(logger.trace(s"[$sinkName] Updating sink context to: ${batch.updatedCommitContext}"))
          _ <- flush(batch.batch.toSeq)
        } yield updateCommitContextPostCommit(currentCommitContext = batch.updatedCommitContext))
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

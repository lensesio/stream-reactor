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

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
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
  errorThreshold:   Int,
  tidyJson:         Boolean,
  errorReporter:    ReportingController[HttpFailureConnectorSpecificRecordData],
  successReporter:  ReportingController[HttpSuccessConnectorSpecificRecordData],
  commitContextRef: Ref[IO, HttpCommitContext],
) extends LazyLogging {

  // TODO: feedback to kafka a warning if the queue gets too large

  // adds records to the queue.  Returns immediately - processing occurs asynchronously.
  def add(newRecords: NonEmptySeq[RenderedRecord]): IO[Unit] =
    recordsQueue.enqueueAll(newRecords)

  def process(): IO[Unit] = {
    for {
      batchInfo <- recordsQueue.popBatch()
      _ <- batchInfo match {
        case EmptyBatchInfo(totalQueueSize) =>
          IO(logger.debug(s"[$sinkName] No batch yet, queue size: $totalQueueSize"))
        case nonEmptyBatchInfo @ NonEmptyBatchInfo(batch, _, totalQueueSize) =>
          processBatch(nonEmptyBatchInfo, batch, totalQueueSize)
      }
    } yield ()
  }.handleErrorWith {
    e =>
      for {
        uniqueError: Option[Throwable] <- addErrorToCommitContext(e)
        _ <- if (uniqueError.nonEmpty) {
          IO(logger.error("Error in HttpWriter", e)) *> IO.raiseError(e)
        } else {
          IO(logger.error("Error in HttpWriter but not reached threshold so ignoring", e)) *> IO.unit
        }
      } yield ()
  }

  private def processBatch(
    nonEmptyBatchInfo: NonEmptyBatchInfo,
    batch:             NonEmptySeq[RenderedRecord],
    totalQueueSize:    Int,
  ) =
    for {
      _ <- IO(
        logger.debug(s"[$sinkName] HttpWriter.process, batch of ${batch.length}, queue size: $totalQueueSize"),
      )
      _                   <- IO.delay(logger.trace(s"[$sinkName] modifyCommitContext for batch of ${nonEmptyBatchInfo.batch.length}"))
      _                   <- flush(nonEmptyBatchInfo.batch)
      updatedCommitContext = updateCommitContextPostCommit(nonEmptyBatchInfo.updatedCommitContext)
      _                   <- IO.delay(logger.trace(s"[$sinkName] Updating sink context to: $updatedCommitContext"))
      _                   <- commitContextRef.set(updatedCommitContext)
      removedElements     <- recordsQueue.dequeue(batch)
      _                   <- resetErrorsInCommitContext()
    } yield removedElements

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

  private def addErrorToCommitContext(e: Throwable): IO[Option[Throwable]] =
    commitContextRef.getAndUpdate {
      commitContext => commitContext.addError(e)
    }.map(cc =>
      cc
        .errors
        .maxByOption { case (_, errSeq) => errSeq.size }
        .filter { case (_, errSeq) => errSeq.size > errorThreshold }
        .flatMap {
          case (_, errSeq) => errSeq.headOption
        },
    )

  private def resetErrorsInCommitContext(): IO[Unit] =
    commitContextRef.getAndUpdate {
      commitContext => commitContext.resetErrors
    } *> IO.unit

  private def flush(records: NonEmptySeq[RenderedRecord]): IO[ProcessedTemplate] =
    for {
      processed  <- IO.fromEither(template.process(records, tidyJson))
      httpResult <- sender.sendHttpRequest(processed)
      _          <- reportResult(records, processed, httpResult)
    } yield processed

  private def reportResult(
    renderedRecords:   NonEmptySeq[RenderedRecord],
    processedTemplate: ProcessedTemplate,
    responseIo:        Either[HttpResponseFailure, HttpResponseSuccess],
  ): IO[Unit] = {
    val maxRecord = OffsetMergeUtils.maxRecord(renderedRecords.toSeq)

    def reportRecord[C <: ConnectorSpecificRecordData]: C => ReportingRecord[C] = (connectorSpecific: C) =>
      new ReportingRecord[C](
        maxRecord.topicPartitionOffset.toTopicPartition.toKafka,
        maxRecord.topicPartitionOffset.offset.value,
        maxRecord.timestamp,
        processedTemplate.endpoint,
        processedTemplate.content,
        connectorSpecific,
      )

    responseIo match {
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

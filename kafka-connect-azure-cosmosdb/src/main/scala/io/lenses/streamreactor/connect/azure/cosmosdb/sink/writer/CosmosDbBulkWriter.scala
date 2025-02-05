/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter.CosmosDbBulkRecordConverter
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

class CosmosDbBulkWriter(
  config:              Kcql,
  recordsQueue:        CosmosRecordsQueue[PendingRecord],
  bulkRecordConverter: CosmosDbBulkRecordConverter,
  queueProcessor:      CosmosDbQueueProcessor,
) extends CosmosDbWriter
    with StrictLogging {

  // adds records to the queue.  Returns immediately - processing occurs asynchronously.
  def insert(newRecords: Seq[SinkRecord]): Either[Throwable, Unit] =
    for {
      _      <- unrecoverableError().toLeft[Unit](())
      insert <- Try(recordsQueue.enqueueAll(newRecords.map(bulkRecordConverter.convert(config, _)).toList)).toEither

    } yield insert

  override def preCommit(
    offsetAndMetadatas: Map[TopicPartition, OffsetAndMetadata],
  ): Map[TopicPartition, OffsetAndMetadata] = queueProcessor.preCommit(offsetAndMetadatas)

  override def unrecoverableError(): Option[Throwable] = queueProcessor.unrecoverableError()
}

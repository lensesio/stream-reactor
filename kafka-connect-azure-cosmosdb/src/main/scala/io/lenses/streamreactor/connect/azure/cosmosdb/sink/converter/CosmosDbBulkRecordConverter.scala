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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter

import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosBulkOperations
import com.azure.cosmos.models.CosmosItemOperation
import com.azure.cosmos.models.CosmosItemRequestOptions
import com.azure.cosmos.models.PartitionKey
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.PendingRecord
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.apache.kafka.connect.sink.SinkRecord

class CosmosDbBulkRecordConverter(
  settings: CosmosDbSinkSettings,
) {

  private val requestOptionsInsert: CosmosItemRequestOptions = new CosmosItemRequestOptions()
    .setConsistencyLevel(settings.consistency)

  def convert(kcql: Kcql, sinkRecord: SinkRecord): Either[Throwable, PendingRecord] =
    for {
      doc    <- sinkRecordToDocument(sinkRecord)
      itemOp <- documentToItemOperation(kcql, doc)
    } yield PendingRecord(
      Topic(sinkRecord.topic()).withPartition(sinkRecord.kafkaPartition()).atOffset(sinkRecord.kafkaOffset()),
      doc,
      itemOp,
    )

  private def sinkRecordToDocument(
    sinkRecord: SinkRecord,
  ): Either[Throwable, Document] =
    SinkRecordToDocument(
      sinkRecord,
      settings.fields(sinkRecord.topic()),
      settings.ignoredField(sinkRecord.topic()),
      settings.keySource,
    )

  private def documentToItemOperation(
    config:   Kcql,
    document: Document,
  ): Either[Throwable, CosmosItemOperation] =
    Right(
      config.getWriteMode match {
        case WriteModeEnum.INSERT =>
          CosmosBulkOperations.getCreateItemOperation(
            document,
            new PartitionKey(document.getId),
            requestOptionsInsert,
          )

        case WriteModeEnum.UPSERT =>
          CosmosBulkOperations.getUpsertItemOperation(
            document,
            new PartitionKey(document.getId),
            requestOptionsInsert,
          )
      },
    )

}

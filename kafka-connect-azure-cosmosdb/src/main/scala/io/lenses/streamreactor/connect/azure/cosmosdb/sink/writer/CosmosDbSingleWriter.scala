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

import com.azure.cosmos.CosmosClient
import com.azure.cosmos.models.CosmosItemRequestOptions
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.kcql.WriteModeEnum
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter.SinkRecordToDocument
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

class CosmosDbSingleWriter(
  config:         Kcql,
  settings:       CosmosDbSinkSettings,
  documentClient: CosmosClient,
) extends CosmosDbWriter
    with StrictLogging {

  private val requestOptionsInsert: CosmosItemRequestOptions = new CosmosItemRequestOptions()
    .setConsistencyLevel(settings.consistency)

  /**
   * Write SinkRecords to Azure Document Db
   *
   * @param records A list of SinkRecords from Kafka Connect to write.
   * @return boolean indication successful write.
   */
  def insert(records: Seq[SinkRecord]): Either[Throwable, Unit] =
    Try {
      records.foreach { record =>
        val document =
          SinkRecordToDocument(
            record,
            settings.fields(record.topic()),
            settings.ignoredField(record.topic()),
            settings.keySource,
          )

        config.getWriteMode match {
          case WriteModeEnum.INSERT =>
            documentClient.getDatabase(settings.database).getContainer(config.getTarget).createItem(
              document,
              requestOptionsInsert,
            )

          case WriteModeEnum.UPSERT =>
            documentClient.getDatabase(settings.database).getContainer(config.getTarget).upsertItem(
              document,
              requestOptionsInsert,
            )

          case WriteModeEnum.UPDATE =>
            // TODO: What behaviour?  Currently this was producing a matcher error
            throw new NotImplementedError("this behaviour hasn't been implemented yet")
        }
      }
    }.toEither

  override def preCommit(
    offsetAndMetadatas: Map[TopicPartition, OffsetAndMetadata],
  ): Map[TopicPartition, OffsetAndMetadata] =
    throw new ConnectException("Not implemented for a single writer - no need for offset management")

  override def unrecoverableError(): Option[Throwable] = Option.empty
}

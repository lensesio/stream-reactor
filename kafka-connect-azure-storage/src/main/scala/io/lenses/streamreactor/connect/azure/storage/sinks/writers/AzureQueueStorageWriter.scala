/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package io.lenses.streamreactor.connect.azure.storage.sinks.writers

import io.lenses.kcql.FormatType
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.common.schemas.SinkRecordConverterHelper.SinkRecordExtension
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageSettings
import io.lenses.streamreactor.connect.azure.storage.getQueueReferences
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try
import com.azure.storage.queue.QueueClient
import com.azure.storage.queue.implementation.models.QueueMessage

object AzureQueueStorageWriter {
  def apply(settings: AzureStorageSettings,
            queueClient: QueueClient): AzureQueueStorageWriter =
    new AzureQueueStorageWriter(settings, queueClient)
}

class AzureQueueStorageWriter(settings: AzureStorageSettings,
                              client: QueueClient)
    extends Writer
    with StrictLogging
    with ErrorHandler {

  //initialize error tracker
  initialize(settings.projections.errorRetries, settings.projections.errorPolicy)
  val objectMapper = new ObjectMapper()
  val jsonConverter = new SimpleJsonConverter()

  private val cloudQueueMap: Map[String, CloudQueue] = {
    val queueConfigs = settings.projections.targets
      .map{ case (topic, queue) => queue -> (settings.projections.autoCreate(topic), settings.encode(queue)) }
    getQueueReferences(client, queueConfigs)
  }

  override def write(queueName: String, records: Seq[SinkRecord], batchSize: Int): Unit = {
    records.foreach(r => {
      Option(r.value()) match {
        case Some(_) =>
          handleTry(Try(cloudQueueMap(queueName).addMessage(convert(r))))
        case None =>
          logger.warn(
            s"Empty payload received on topic [${r.topic()}]. No payload set. Message discarded")
      }
    })
  }

  def convert(record: SinkRecord): QueueMessage = {
    settings.projections.formats(record.topic()) match {
      case FormatType.JSON => new QueueMessage(toJson(record))
      case FormatType.BINARY =>
        new QueueMessage(toJson(record).getBytes())
      case _ =>
        throw new ConnectException(
          s"Unknown WITHFORMAT type [${settings.projections.formats(record.topic()).toString}]")
    }
  }

  def toJson(record: SinkRecord): String = {
    val filtered = record.newFilteredRecordAsStruct(settings.projections)
    jsonConverter.fromConnectData(filtered.schema(), filtered).toString
  }
}

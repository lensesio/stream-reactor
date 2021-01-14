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

import com.datamountaineer.kcql.FormatType
import com.datamountaineer.streamreactor.connect.converters.{FieldConverter, Transform}
import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.microsoft.azure.storage.queue.{CloudQueue, CloudQueueClient, CloudQueueMessage}
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.storage.config.AzureStorageSettings
import io.lenses.streamreactor.connect.azure.storage.getQueueReferences
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.util.Try

object AzureQueueStorageWriter {
  def apply(settings: AzureStorageSettings,
            queueClient: CloudQueueClient): AzureQueueStorageWriter =
    new AzureQueueStorageWriter(settings, queueClient)
}

class AzureQueueStorageWriter(settings: AzureStorageSettings,
                              client: CloudQueueClient)
    extends Writer
    with StrictLogging
    with ErrorHandler
    with ConverterUtil {

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private val cloudQueueMap: Map[String, CloudQueue] = {
    val queueConfigs = settings.targets.values
      .map(q => q -> (settings.autocreate(q), settings.encode(q)))
      .toMap
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

  def convert(record: SinkRecord): CloudQueueMessage = {
    settings.formatType(record.topic()) match {
      case FormatType.JSON => new CloudQueueMessage(toJsonString(record))
      case FormatType.BINARY =>
        new CloudQueueMessage(toJsonString(record).getBytes())
      case _ =>
        throw new ConnectException(
          s"Unknown WITHFORMAT type [${settings.formatType(record.topic()).toString}]")
    }
  }

  def toJsonString(record: SinkRecord): String = {
    Transform(settings.fieldsMap(record.topic()).map(FieldConverter.apply),
              Seq.empty,
              record.valueSchema(),
              record.value(),
              withStructure = false)
  }
}

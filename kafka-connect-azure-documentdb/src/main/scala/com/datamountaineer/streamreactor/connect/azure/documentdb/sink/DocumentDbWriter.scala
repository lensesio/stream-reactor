/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import com.datamountaineer.connector.config.WriteModeEnum
import com.datamountaineer.streamreactor.connect.azure.documentdb.DocumentClientProvider
import com.datamountaineer.streamreactor.connect.azure.documentdb.config.{DocumentDbConfig, DocumentDbSinkSettings}
import com.datamountaineer.streamreactor.connect.errors.{ErrorHandler, ErrorPolicyEnum}
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.microsoft.azure.documentdb._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}

import scala.util.{Failure, Success, Try}

/**
  * <h1>DocumentDbWriter</h1>
  * Azure DocumentDb Json writer for Kafka connect
  * Writes a list of Kafka connect sink records to Azure DocumentDb using the JSON support.
  */
class DocumentDbWriter(settings: DocumentDbSinkSettings, documentClient: DocumentClient) extends StrictLogging with ConverterUtil with ErrorHandler {
  private val database = Try(documentClient.readDatabase(settings.database, new RequestOptions()).getResource) match {
    case Failure(e) => throw new RuntimeException(s"Could not identify database ${settings.database}", e)
    case Success(d) => d
  }

  private val configMap = settings.kcql
    .map { c =>
      Option(documentClient.readCollection(c.getTarget, null).getResource).getOrElse {
        throw new IllegalArgumentException(s"Collection '${c.getTarget}' not found!")
      }
      c.getSource -> c
    }.toMap


  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  /**
    * Write SinkRecords to Azure Document Db.
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    **/
  def write(records: Seq[SinkRecord]): Unit = {
    if (records.nonEmpty) {
      insert(records)
    }
  }

  /**
    * Write SinkRecords to Azure Document Db
    *
    * @param records A list of SinkRecords from Kafka Connect to write.
    * @return boolean indication successful write.
    **/
  private def insert(records: Seq[SinkRecord]) = {
    try {
      records.groupBy(_.topic()).foreach { case (topic, groupedRecords) =>
        val config = configMap(topic)
        groupedRecords.foreach { record =>
          val (document, keysAndValues) = SinkRecordToDocument(
            record,
            settings.keyBuilderMap.getOrElse(record.topic(), Set.empty)
          )(settings)

          val config = configMap.getOrElse(record.topic(), sys.error(s"${record.topic()} is not handled by the configuration."))
          config.getWriteMode match {
            case WriteModeEnum.INSERT =>
              documentClient.createDocument(config.getTarget, document, null, false).getResource

            case WriteModeEnum.UPSERT =>
              documentClient.upsertDocument(config.getTarget, document, null, false).getResource
          }
        }
      }
    }
    catch {
      case t: Throwable =>
        logger.error(s"There was an error inserting the records ${t.getMessage}", t)
        handleTry(Failure(t))
    }
  }

  def close(): Unit = {
    logger.info("Shutting down Document DB writer.")
    documentClient.close()
  }
}


//Factory to build
object DocumentDbWriter extends StrictLogging {
  def apply(connectorConfig: DocumentDbConfig, context: SinkTaskContext): DocumentDbWriter = {

    implicit val settings = DocumentDbSinkSettings(connectorConfig)
    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(connectorConfig.getLong(DocumentDbConfig.ERROR_RETRY_INTERVAL_CONFIG))
    }

    logger.info(s"Initialising Document Db writer.")
    new DocumentDbWriter(settings, DocumentClientProvider.get(settings))
  }
}

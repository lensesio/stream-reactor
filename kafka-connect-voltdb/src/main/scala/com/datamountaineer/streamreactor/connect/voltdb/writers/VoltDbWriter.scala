/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.voltdb.writers

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.sink.DbWriter
import com.datamountaineer.streamreactor.connect.voltdb.config.VoltSettings
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException
import org.voltdb.client.{ClientConfig, ClientFactory}

import scala.util.Try

class VoltDbWriter(settings: VoltSettings) extends DbWriter with StrictLogging with ConverterUtil with ErrorHandler {

  //ValidateStringParameterFn(settings.servers, "settings")
  //ValidateStringParameterFn(settings.user, "settings")

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private val voltConfig = new ClientConfig(settings.user, settings.password)
  private val client = ClientFactory.createClient(voltConfig)
  VoltConnectionConnectFn(client, settings)

  private val proceduresMap = settings.fieldsExtractorMap.values.map { extract =>
    val procName = s"${extract.targetTable}.${if (extract.isUpsert) "upsert" else "insert"}"
    logger.info(s"Retrieving the metadata for $procName ...")
    val fields = VoltDbMetadataReader.getProcedureParameters(client, extract.targetTable).map(_.toUpperCase)
    logger.info(s"$procName expected arguments are: ${fields.mkString(",")}")
    extract.targetTable -> ProcAndFields(procName, fields)
  }.toMap

  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      val t = Try(records.withFilter(_.value() != null).foreach(insert))
      t.foreach(_ => logger.info("Writing complete"))
      handleTry(t)
    }
  }

  private def insert(record: SinkRecord) = {
    require(record.value().getClass == classOf[Struct], "Only Struct payloads are handled")
    val extractor = settings.fieldsExtractorMap.getOrElse(record.topic(),
      throw new ConfigException(s"${record.topic()} is not handled by the configuration:${settings.fieldsExtractorMap.keys.mkString(",")}"))

    val fieldsAndValuesMap = extractor.get(record.value().asInstanceOf[Struct]).map { case (k, v) => (k.toUpperCase, v) }
    logger.info(fieldsAndValuesMap.mkString(","))
    val procAndFields: ProcAndFields = proceduresMap(extractor.targetTable)
    //get the list of arguments to pass to the table insert/upsert procedure. if the procedure expects a field and is
    //not present in the incoming SinkRecord it would use null
    //No table evolution is supported yet

    val arguments: Array[String] = PrepareProcedureFieldsFn(procAndFields.fields, fieldsAndValuesMap).toArray
    logger.info(s"Calling procedure:${procAndFields.procName} with parameters:${procAndFields.fields.mkString(",")} with arguments:${arguments.mkString(",")}")

    client.callProcedure(procAndFields.procName, arguments: _*)
  }

  override def close(): Unit = client.close()

  private case class ProcAndFields(procName: String, fields: Seq[String])

}

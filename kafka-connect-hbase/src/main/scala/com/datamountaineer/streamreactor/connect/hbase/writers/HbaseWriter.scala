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

package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.hbase._
import com.datamountaineer.streamreactor.connect.hbase.config.HBaseSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datamountaineer.streamreactor.connect.sink.DbWriter
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.util.Try

class HbaseWriter(settings: HBaseSettings, hbaseConfig: Configuration) extends DbWriter
  with StrictLogging with ConverterUtil with ErrorHandler {


  ValidateStringParameterFn(settings.columnFamilyMap, "columnFamily")
  //ValidateStringParameterFn(tableName, "tableName")
  private val columnFamilyBytes = Bytes.toBytes(settings.columnFamilyMap)
  private val routeMapping = settings.routes

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private var columnsBytesMap = Map.empty[String, Array[Byte]]
  private var connection = createConnection()
  private val tables = routeMapping.map(rm => (rm.getSource, connection.getTable(TableName.valueOf(rm.getTarget)))).toMap
  private val rowKeyMap = settings.rowKeyModeMap

  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug(s"Received ${records.size} records.")

      if (connection.isClosed) {
        val t = Try(createConnection())
        val dt = handleTry(t)
        connection = dt.get
      }

      val grouped = records.groupBy(_.topic())
      insert(grouped)
    }
  }

  def createConnection(): Connection = {
    ConnectionFactory.createConnection(hbaseConfig)
  }

  /**
    * Insert the sinkrecords into HBase.
    *
    * Build a put from the extracted fields and values for each record.
    * */
  def insert(records: Map[String, Seq[SinkRecord]]): Unit = {

    records.foreach({
      case (topic, sinkRecords : Seq[SinkRecord]) =>
        val table = tables(topic)
        val puts = sinkRecords.flatMap { record =>

          require(record.value() != null && record.value().getClass == classOf[Struct],
            "The SinkRecord payload should be of type Struct")

          val keyBuilder = rowKeyMap(topic)
          val extractor = settings.extractorFields(topic)
          val fieldsAndValues = extractor.get(record.value.asInstanceOf[Struct])

          if (fieldsAndValues.nonEmpty) {
            val put = new Put(keyBuilder.build(record, null))
            fieldsAndValues.foreach { case (fieldName, bytes) =>
              //build the map of fields byte representation
              if (!columnsBytesMap.contains(fieldName)) {
                columnsBytesMap = columnsBytesMap + (fieldName -> Bytes.toBytes(fieldName))
              }
              put.addColumn(columnFamilyBytes, columnsBytesMap(fieldName), bytes)
            }
            Some(put)
          }
          else {
            None
          }
        }
        logger.debug(s"Writing ${puts.size} rows to Hbase...")

        val t = Try(table.put(puts.asJava))
        handleTry(t)
        logger.debug(s"Wrote ${puts.size} rows.")
    })
  }

  override def close(): Unit = {
    tables.foreach({case (_, table) => table.close()})

    if (connection != null) {
      connection.close()
    }
  }
}

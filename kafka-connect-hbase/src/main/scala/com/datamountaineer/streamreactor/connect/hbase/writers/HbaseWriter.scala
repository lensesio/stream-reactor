/**
  * Copyright 2015 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.hbase.writers

import com.datamountaineer.streamreactor.connect.hbase._
import com.datamountaineer.streamreactor.connect.sink.DbWriter
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConversions._

class HbaseWriter(val columnFamily: String,
                  val tableName: String,
                  val fieldsExtractor: FieldsValuesExtractor,
                  val rowKeyBuilder: RowKeyBuilder
                 ) extends DbWriter with StrictLogging {
  ValidateStringParameterFn(columnFamily, "columnFamily")
  ValidateStringParameterFn(tableName, "tableName")

  private val columnFamilyBytes = Bytes.toBytes(columnFamily)

  /**
    * Cache for the columns as array of bytes
    */
  private var columnsBytesMap = Map.empty[String, Array[Byte]]

  /**
    * The hbase client connection
    */
  private lazy val connection = ConnectionFactory.createConnection(HBaseConfiguration.create())
  private lazy val table = connection.getTable(TableName.valueOf(tableName))


  override def write(records: Seq[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.warn("Received empty sequence of SinkRecord")
    }
    else {
      val puts = records.flatMap { record =>

        logger.debug(s"Received recrod from topic:${record.topic()} partition:${record.kafkaPartition()} " +
          s"and offset:${record.kafkaOffset()}")
        require(record.value() != null && record.value().getClass == classOf[Struct],
          "The SinkRecord payload should be of type Struct")
        //logger.info(s"Received record with value schema:${record.valueSchema()} and key schema:${record.keySchema()}
        // and value:${record.value().getClass}-${record.value()}")

        val fieldsAndValues = fieldsExtractor.get(record.value.asInstanceOf[Struct])

        if (fieldsAndValues.nonEmpty) {
          val put = new Put(rowKeyBuilder.build(record, null))
          fieldsAndValues.foreach { case (fieldName, bytes) =>
            //build the map of fields byte representation
            if (!columnsBytesMap.contains(fieldName)) {
              columnsBytesMap = columnsBytesMap + (fieldName -> Bytes.toBytes(fieldName))
            }
            put.addColumn(columnFamilyBytes, columnsBytesMap.get(fieldName).get, bytes)
          }
          Some(put)
        }
        else {
          None
        }
      }
      logger.info(s"Writing ${puts.size} rows to Hbase...")
      table.put(puts)
      logger.info(s"Wrote ${puts.size} rows.")
    }
  }

  override def close(): Unit = {
    if (table != null) {
      table.close()
    }

    if (connection != null) {
      connection.close()
    }
  }
}

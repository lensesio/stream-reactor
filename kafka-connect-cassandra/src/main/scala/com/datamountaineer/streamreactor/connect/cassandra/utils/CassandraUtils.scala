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

package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.datamountaineer.connector.config.Config
import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.{Cluster, ColumnDefinitions, DataType, Row}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct, Timestamp}
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 21/04/16.
  * stream-reactor
  */
object CassandraUtils {
  val mapper = new ObjectMapper()
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

  val OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().build()
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()

  /**
    * Check if we have tables in Cassandra and if we have table named the same as our topic
    *
    * @param cluster  A Cassandra cluster to check on
    * @param routes   A list of route mappings
    * @param keySpace The keyspace to look in for the tables
    **/
  def checkCassandraTables(cluster: Cluster, routes: Set[Config], keySpace: String): Unit = {
    val metaData = cluster.getMetadata.getKeyspace(keySpace).getTables
    val tables: Set[String] = metaData.map(t => t.getName).toSet
    val topics = routes.map(rm => rm.getTarget)

    //check tables
    if (tables.isEmpty) throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")

    //check we have a table for all topics
    val missing = topics.diff(tables)

    if (missing.nonEmpty) throw new ConnectException(s"No tables found in Cassandra for topics ${missing.mkString(",")}")
  }


  /**
    * get the columns that are to be placed in the Source Record
    * by removing the ignore columns from the select columns
    *
    * @return the comma separated columns
    */
  def getStructColumns(row: Row, ignoreList: List[String]): List[ColumnDefinitions.Definition] = {
    //TODO do we need to get the list of columns everytime?
    val cols = row.getColumnDefinitions

    val colFiltered = if (ignoreList != null && ignoreList.nonEmpty) {
      cols.filter(cd => !ignoreList.contains(cd.getName)).toList
    }
    else {
      cols.toList
    }
    colFiltered
  }

  /**
    * Convert a Cassandra row to a SourceRecord
    *
    * @param row The Cassandra resultset row to convert
    * @return a SourceRecord
    **/
  def convert(row: Row, schemaName: String, colDefList: List[ColumnDefinitions.Definition]): Struct = {
    val connectSchema = convertToConnectSchema(colDefList, schemaName)
    val struct = new Struct(connectSchema)
    if (colDefList != null) {
      colDefList.map { c =>
        val value = mapTypes(c, row)
        struct.put(c.getName, value)
      }.head
    }
    struct
  }

  /**
    * Extract the Cassandra data type can convert to the Connect type
    *
    * @param columnDef The cassandra column def to convert
    * @param row       The cassandra row to extract the data from
    * @return The converted value
    **/
  def mapTypes(columnDef: Definition, row: Row): Any = {
    columnDef.getType.getName match {
      case DataType.Name.DECIMAL => row.getFloat(columnDef.getName).toString
      case DataType.Name.ASCII | DataType.Name.TEXT | DataType.Name.VARCHAR => row.getString(columnDef.getName)
      case DataType.Name.INET => row.getInet(columnDef.getName).toString
      case DataType.Name.MAP => mapper.writeValueAsString(row.getMap(columnDef.getName, classOf[String], classOf[String]))
      case DataType.Name.LIST => mapper.writeValueAsString(row.getList(columnDef.getName, classOf[String]))
      case DataType.Name.SET => mapper.writeValueAsString(row.getSet(columnDef.getName, classOf[String]))
      case DataType.Name.UUID =>
        //need to convert to string since the schema is set to String
        Option(row.getUUID(columnDef.getName)).map(_.toString).orNull
      case DataType.Name.BLOB => row.getBytes(columnDef.getName)
      case DataType.Name.TINYINT | DataType.Name.SMALLINT => row.getShort(columnDef.getName)
      case DataType.Name.INT => row.getInt(columnDef.getName)
      case DataType.Name.DOUBLE => row.getDouble(columnDef.getName)
      case DataType.Name.FLOAT => row.getFloat(columnDef.getName)
      case DataType.Name.COUNTER | DataType.Name.BIGINT | DataType.Name.VARINT => row.getLong(columnDef.getName)
      case DataType.Name.BOOLEAN => row.getBool(columnDef.getName)
      case DataType.Name.DATE => Option(row.getDate(columnDef.getName))
        .map(d => new Date(d.getMillisSinceEpoch))
        .orNull
      case DataType.Name.TIME => row.getTime(columnDef.getName)
      case DataType.Name.TIMESTAMP =>
        Option(row.getTimestamp(columnDef.getName))
          .orNull
      case DataType.Name.TUPLE => row.getTupleValue(columnDef.getName).toString
      case DataType.Name.UDT => row.getUDTValue(columnDef.getName).toString
      case DataType.Name.TIMEUUID => row.getUUID(columnDef.getName).toString
      case a@_ => throw new ConnectException(s"Unsupported Cassandra type $a.")
    }
  }

  /**
    * Convert a set of CQL columns from a Cassandra row to a
    * Connect schema
    *
    * @param cols A set of Column Definitions
    * @return a Connect Schema
    **/
  def convertToConnectSchema(cols: List[Definition], name: String): Schema = {
    val builder = SchemaBuilder.struct().name(name)
    if (cols != null) cols.map(c => builder.field(c.getName, typeMapToConnect(c)))
    builder.build()
  }


  /**
    * Map the Cassandra DataType to the Connect types
    *
    * @param columnDef The cassandra column definition
    * @return the Connect schema type
    **/
  def typeMapToConnect(columnDef: Definition): Schema = {
    columnDef.getType.getName match {
      case DataType.Name.TIMEUUID |
           DataType.Name.UUID |
           DataType.Name.INET |
           DataType.Name.ASCII |
           DataType.Name.TEXT |
           DataType.Name.VARCHAR |
           DataType.Name.TUPLE |
           DataType.Name.UDT => Schema.OPTIONAL_STRING_SCHEMA

      case DataType.Name.DATE => OPTIONAL_DATE_SCHEMA
      case DataType.Name.BOOLEAN => Schema.OPTIONAL_BOOLEAN_SCHEMA
      case DataType.Name.TINYINT => Schema.OPTIONAL_INT8_SCHEMA
      case DataType.Name.SMALLINT => Schema.OPTIONAL_INT16_SCHEMA
      case DataType.Name.TIMESTAMP => OPTIONAL_TIMESTAMP_SCHEMA
      case DataType.Name.INT => Schema.OPTIONAL_INT32_SCHEMA
      case DataType.Name.DECIMAL => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.DOUBLE => Schema.OPTIONAL_FLOAT64_SCHEMA
      case DataType.Name.FLOAT => Schema.OPTIONAL_FLOAT32_SCHEMA
      case DataType.Name.COUNTER | DataType.Name.BIGINT | DataType.Name.VARINT | DataType.Name.TIME => Schema.OPTIONAL_INT64_SCHEMA
      case DataType.Name.BLOB => Schema.OPTIONAL_BYTES_SCHEMA
      case DataType.Name.MAP => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.LIST => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.SET => Schema.OPTIONAL_STRING_SCHEMA
      case a@_ => throw new ConnectException(s"Unsupported Cassandra type $a.")
    }
  }
}

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

package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.text.SimpleDateFormat
import java.util.Date

import com.datastax.driver.core.{Cluster, DataType, Row}
import com.datastax.driver.core.ColumnDefinitions.Definition
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 21/04/16.
  * stream-reactor
  */
object CassandraUtils {
  val mapper = new ObjectMapper()
  private val defaultTimestamp = "1900-01-01 00:00:00+0000"
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")

  /**
    * Check if we have tables in Cassandra and if we have table named the same as our topic
    *
    * @param cluster A Cassandra cluster to check on
    * @param topics A list of the assigned topics
    * @param keySpace The keyspace to look in for the tables
    * */
  def checkCassandraTables(cluster: Cluster , topics: Set[String], keySpace: String) : Unit = {
    val metaData = cluster.getMetadata.getKeyspace(keySpace).getTables.asScala
    val tables: Set[String] = metaData.map(t=>t.getName).toSet

    //check tables
    if (tables.isEmpty) throw new ConnectException(s"No tables found in Cassandra for keyspace $keySpace")

    //check we have a table for all topics
    val missing = topics.diff(tables)

    if (missing.nonEmpty) throw new ConnectException(s"Not table found in Cassandra for topics ${missing.mkString(",")}")
  }


//  def getTimeUUIDColumns(cluster: Cluster, tables: Set[String], keySpace: String) : Map[String, String] = {
//    val metaData = cluster.getMetadata.getKeyspace(keySpace).getTables.asScala
//    metaData.map(
//      t=>(t.getName,
//          t.getColumns.asScala
//            .filter(c=>c.getType.equals(DataType.Name.TIMEUUID))
//            .map(c=>c.getName)
//        )
//    ).toMap
//  }

  /**
    * Convert a Cassandra row to a SourceRecord
    *
    * @param row The Cassandra resultset row to convert
    * @return a SourceRecord
    * */
  def convert(row: Row) : Struct = {
    //TODO do we need to get the list of columns everytime?

    val cols = row.getColumnDefinitions.asScala
    val connectSchema = convertToConnectSchema(cols.toList)
    val struct = new Struct(connectSchema)

    cols.map(c => {
      struct.put(c.getName, mapTypes(c, row))
    }).head
    struct
  }

  /**
    * Extract the Cassandra data type can convert to the Connect type
    *
    * @param columnDef The cassandra column def to convert
    * @param row The cassandra row to extract the data from
    * @return The converted value
    * */
  def mapTypes(columnDef: Definition, row: Row) : Any = {
    columnDef.getType.getName match {
      case DataType.Name.DECIMAL => row.getFloat(columnDef.getName).toString
      case DataType.Name.ASCII | DataType.Name.TEXT | DataType.Name.VARCHAR => row.getString(columnDef.getName)
      case DataType.Name.INET => row.getInet(columnDef.getName).toString
      case DataType.Name.MAP => mapper.writeValueAsString(row.getMap(columnDef.getName, classOf[String], classOf[String]))
      case DataType.Name.LIST => mapper.writeValueAsString(row.getList(columnDef.getName, classOf[String]))
      case DataType.Name.SET  => mapper.writeValueAsString(row.getSet(columnDef.getName, classOf[String]))
      case DataType.Name.UUID => row.getString(columnDef.getName)
      case DataType.Name.BLOB => row.getBytes(columnDef.getName)
      case DataType.Name.TINYINT | DataType.Name.SMALLINT => row.getShort(columnDef.getName)
      case DataType.Name.INT => row.getInt(columnDef.getName)
      case DataType.Name.FLOAT => row.getFloat(columnDef.getName)
      case DataType.Name.COUNTER | DataType.Name.BIGINT | DataType.Name.VARINT | DataType.Name.DOUBLE => row.getLong(columnDef.getName)
      case DataType.Name.BOOLEAN => row.getBool(columnDef.getName)
      case DataType.Name.DATE => row.getDate(columnDef.getName).toString
      case DataType.Name.TIME => row.getTime(columnDef.getName)
      case DataType.Name.TIMESTAMP =>
        val timestamp: Date = row.getTimestamp(columnDef.getName)
        dateFormatter.format(timestamp)
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
    * */
  def convertToConnectSchema(cols: List[Definition]) : Schema = {
    val builder = SchemaBuilder.struct
    cols.map(c=>builder.field(c.getName, typeMapToConnect(c)))
    builder.build()
  }


  /**
    * Map the Cassandra DataType to the Connect types
    *
    * @param columnDef The cassandra column definition
    * @return the Connect schema type
    * */
  def typeMapToConnect(columnDef: Definition) : Schema =
  {
    columnDef.getType.getName match {
      case DataType.Name.TIMEUUID | DataType.Name.UUID | DataType.Name.INET | DataType.Name.ASCII |
           DataType.Name.TEXT | DataType.Name.VARCHAR | DataType.Name.TIMESTAMP | DataType.Name.DATE |
           DataType.Name.TUPLE | DataType.Name.UDT => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.BOOLEAN => Schema.OPTIONAL_BOOLEAN_SCHEMA
      case DataType.Name.TINYINT => Schema.OPTIONAL_INT8_SCHEMA
      case DataType.Name.SMALLINT => Schema.OPTIONAL_INT16_SCHEMA
      case DataType.Name.INT => Schema.OPTIONAL_INT32_SCHEMA
      case DataType.Name.DECIMAL => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.FLOAT => Schema.OPTIONAL_FLOAT32_SCHEMA
      case DataType.Name.COUNTER | DataType.Name.BIGINT | DataType.Name.VARINT| DataType.Name.DOUBLE |
           DataType.Name.TIME => Schema.OPTIONAL_INT64_SCHEMA
      case DataType.Name.BLOB => Schema.OPTIONAL_BYTES_SCHEMA
      case DataType.Name.MAP => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.LIST => Schema.OPTIONAL_STRING_SCHEMA
      case DataType.Name.SET => Schema.OPTIONAL_STRING_SCHEMA
      case a@_ => throw new ConnectException(s"Unsupported Cassandra type $a.")
    }
  }


  /**
    * Break a comma and colon separated string into a map of table to topic or topic to table
    *
    * If now values is found after a comma the value before the comma is used.
    *
    * @param input The input string to parse.
    * @return a Map of table->topic or topic->table.
    * */
  def tableTopicParser(input: String) : Map[String, String] = {
    input.split(",")
      .toList
      .map(c=>c.split(":"))
      .map(a => {if (a.length == 1) (a(0), a(0)) else (a(0), a(1)) }).toMap
  }


//  /**
//    * Get the current time of a cassandra database, CQL requires a table so limit by 1
//    * Can't do select now() like a RDBMS
//    *
//    * @param session Cassandra session to execute the query
//    * @param keySpace The keyspace of the table
//    * @param table The table.
//    * */
//  def getNowOnDb(session: Session, keySpace: String, table: String) : Date = {
//    val rsNow = session.execute(s"SELECT toTimestamp(now()) AS now FROM ${keySpace}.${table} LIMIT 1;")
//    rsNow.one().getTimestamp("now")
//  }
}

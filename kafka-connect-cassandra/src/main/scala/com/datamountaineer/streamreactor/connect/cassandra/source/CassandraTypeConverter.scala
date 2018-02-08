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

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.math.RoundingMode
import java.util.Date

import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSourceSetting
import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.{CodecRegistry, _}
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConversions._

/**
  * Created by caio@caiooliveira.eti.br on 17/01/18.
  * stream-reactor
  */
class CassandraTypeConverter(private val codecRegistry: CodecRegistry,
                             private val setting: CassandraSourceSetting) extends StrictLogging {

  val mapper = new ObjectMapper()
  val OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().build()
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
  val OPTIONAL_DECIMAL_SCHEMA = Decimal.builder(18).optional().build()

  private val mappingCollectionToJson: Boolean = setting.mappingCollectionToJson

  def asJavaType(dataType: DataType): Class[_] = codecRegistry.codecFor(dataType).getJavaType.getRawType

  /**
    * get the columns that are to be placed in the Source Record
    * by removing the ignore columns from the select columns
    *
    * @return the comma separated columns
    */
  def getStructColumns(row: Row, ignoreList: Set[String]): List[ColumnDefinitions.Definition] = {
    //TODO do we need to get the list of columns everytime?
    row.getColumnDefinitions.filter(cd => !ignoreList.contains(cd.getName)).toList
  }

  /**
    * Convert a Cassandra row to a SourceRecord
    *
    * @param row The Cassandra resultset row to convert
    * @return a SourceRecord
    **/
  def convert(row: Row, schemaName: String, colDefList: List[ColumnDefinitions.Definition], schema: Option[Schema]): Struct = {
    val connectSchema = schema.getOrElse(convertToConnectSchema(colDefList, schemaName))
    val struct = new Struct(connectSchema)
    if (colDefList != null) {
      colDefList.foreach { c =>
        val value = mapTypes(c, row)
        struct.put(c.getName, value)
      }
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
  private def mapTypes(columnDef: Definition, row: Row): Any = {
    columnDef.getType.getName match {
      case DataType.Name.DECIMAL =>
        Option(row.getDecimal(columnDef.getName)).map { d =>
          d.setScale(18, RoundingMode.HALF_UP)
        }.orNull
      case DataType.Name.ASCII | DataType.Name.TEXT | DataType.Name.VARCHAR => row.getString(columnDef.getName)
      case DataType.Name.INET => row.getInet(columnDef.getName).toString
      case DataType.Name.MAP | DataType.Name.LIST | DataType.Name.SET  =>
        if (mappingCollectionToJson) mapper.writeValueAsString(collectionMapTypes(columnDef, row))
        else collectionMapTypes(columnDef, row)
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
    * Extract the Cassandra collection data type can convert to the Connect type
    *
    * @param columnDef The cassandra column def to convert
    * @param row       The cassandra row to extract the data from
    * @return The converted value
    **/
  private def collectionMapTypes(columnDef: Definition, row: Row): Any = {
    val dataType = columnDef.getType

    dataType.getName match {
      case DataType.Name.MAP => row.getMap(columnDef.getName, asJavaType(dataType.getTypeArguments.get(0)), asJavaType(dataType.getTypeArguments.get(1)))
      case DataType.Name.LIST => row.getList(columnDef.getName, asJavaType(dataType.getTypeArguments.get(0)))
      case DataType.Name.SET => row.getList(columnDef.getName, asJavaType(dataType.getTypeArguments.get(0)))
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
    if (cols != null) cols.map(c => builder.field(c.getName, typeMapToConnect(c.getType)))
    builder.build()
  }

  /**
    * Map the Cassandra DataType to the Connect types
    *
    * @param dataType The cassandra column definition
    * @return the Connect schema type
    **/
  private def typeMapToConnect(dataType: DataType): Schema = {
    dataType.getName match {
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
      case DataType.Name.DECIMAL => OPTIONAL_DECIMAL_SCHEMA
      case DataType.Name.DOUBLE => Schema.OPTIONAL_FLOAT64_SCHEMA
      case DataType.Name.FLOAT => Schema.OPTIONAL_FLOAT32_SCHEMA
      case DataType.Name.COUNTER | DataType.Name.BIGINT | DataType.Name.VARINT | DataType.Name.TIME => Schema.OPTIONAL_INT64_SCHEMA
      case DataType.Name.BLOB => Schema.OPTIONAL_BYTES_SCHEMA
      case DataType.Name.MAP | DataType.Name.LIST | DataType.Name.SET  => collectionTypeMapToConnect(dataType)
      case a@_ => throw new ConnectException(s"Unsupported Cassandra type $a.")
    }
  }

  /**
    * Map the Cassandra DataType Collection to the Connect types
    *
    * @param dataType The cassandra column definition
    * @return the Connect schema type
    **/
  private def collectionTypeMapToConnect(dataType: DataType): Schema = {

    if(mappingCollectionToJson){
      Schema.OPTIONAL_STRING_SCHEMA
    } else {
      dataType.getName match {
        case DataType.Name.LIST | DataType.Name.SET  => SchemaBuilder.array(
            typeMapToConnect(dataType.getTypeArguments.get(0))
          ).optional().build();
        case DataType.Name.MAP  => SchemaBuilder.map(
            typeMapToConnect(dataType.getTypeArguments.get(0)),
            typeMapToConnect(dataType.getTypeArguments.get(1))
          ).optional().build()
        case a@_ => throw new ConnectException(s"Unsupported Cassandra type $a.")
      }
    }
  }

}



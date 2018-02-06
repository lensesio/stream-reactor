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

package com.datamountaineer.streamreactor.connect.kudu

import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kudu.ColumnSchema
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.client.{KuduTable, PartialRow, Upsert}

import scala.collection.JavaConversions._

trait KuduConverter {

  /**
    * Convert a SinkRecord to a Kudu row upsert for a Kudu Table
    *
    * @param record A SinkRecord to convert
    * @param table  A Kudu table to create a row insert for
    * @return A Kudu upsert operation
    **/
  def convertToKuduUpsert(record: SinkRecord, table: KuduTable): Upsert = {
    val recordFields = record.valueSchema().fields()
    val kuduColNames = table.getSchema.getColumns.map(c => c.getName)
    val upsert = table.newUpsert()
    val row = upsert.getRow
    recordFields
      .filter(f => kuduColNames.contains(f.name())) //handle missing fields in target (maybe dropped)
      .map(f => addFieldToRow(record, f, row))
    upsert
  }

  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param field  SinkRecord Field
    * @param record Sink record
    * @param row    The Kudu row to add the field to
    * @return the updated Kudu row
    **/
  private def addFieldToRow(record: SinkRecord,
                            field: Field,
                            row: PartialRow): PartialRow = {
    val fieldType = field.schema().`type`()
    val fieldName = field.name()
    val struct = record.value().asInstanceOf[Struct]

    field.schema().name() match {
      case Time.LOGICAL_NAME =>
        Option(struct.get(fieldName)).foreach {
          case d: java.util.Date => row.addLong(fieldName, d.getTime)
          case i: Integer => row.addLong(fieldName, i.toLong)
          case other => throw new UnsupportedOperationException(s"Unsupported value ${other.getClass.getCanonicalName}")
        }
      case Timestamp.LOGICAL_NAME =>
        Option(struct.get(fieldName)).foreach {
          case d: java.util.Date => row.addLong(fieldName, d.getTime)
          case l: java.lang.Long => row.addLong(fieldName, l)
          case other => throw new UnsupportedOperationException(s"Unsupported value ${other.getClass.getCanonicalName}")
        }

      case Date.LOGICAL_NAME =>
        Option(struct.get(fieldName)).foreach {
          case d: java.util.Date => row.addLong(fieldName, d.getTime)
          case i: Integer => row.addLong(fieldName, i.toLong)
          case other => throw new UnsupportedOperationException(s"Unsupported value ${other.getClass.getCanonicalName}")
        }
      case Decimal.LOGICAL_NAME =>
        val binary = struct.get(field.name()) match {
          case a: Array[_] => a.asInstanceOf[Array[Byte]]
          case bd: java.math.BigDecimal => Decimal.fromLogical(field.schema(), bd)
          case bd: BigDecimal => Decimal.fromLogical(field.schema(), bd.bigDecimal)
        }
        row.addBinary(fieldName, binary)
      case _ =>
        fieldType match {
          case Type.STRING => row.addString(fieldName, struct.getString(fieldName))
          case Type.INT8 => row.addByte(fieldName, struct.getInt8(fieldName).asInstanceOf[Byte])
          case Type.INT16 => row.addShort(fieldName, struct.getInt16(fieldName))
          case Type.INT32 => row.addInt(fieldName, struct.getInt32(fieldName))
          case Type.INT64 => row.addLong(fieldName, struct.getInt64(fieldName))
          case Type.BOOLEAN => row.addBoolean(fieldName, struct.get(fieldName).asInstanceOf[Boolean])
          case Type.FLOAT32 => row.addFloat(fieldName, struct.getFloat32(fieldName))
          case Type.FLOAT64 => row.addDouble(fieldName, struct.getFloat64(fieldName))
          case Type.BYTES => row.addBinary(fieldName, struct.getBytes(fieldName))
          case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
        }
    }
    row
  }

  /**
    * Convert Connect Schema to Kudu
    *
    * @param record A sinkRecord to get the value schema from
    **/
  def convertToKuduSchema(record: SinkRecord): org.apache.kudu.Schema = {
    val connectFields = record.valueSchema().fields()
    val kuduFields = createKuduColumns(connectFields.toSet)
    new org.apache.kudu.Schema(kuduFields.toList)
  }

  def convertToKuduSchema(schema: Schema): org.apache.kudu.Schema = {
    val connectFields = createKuduColumns(schema.fields().toSet)
    new org.apache.kudu.Schema(connectFields.toList)
  }

  def createKuduColumns(fields: Set[Field]): Set[ColumnSchema] = fields.map(cf => convertConnectField(cf))


  /**
    * Convert a connect schema field to a Kudu field
    *
    * @param field The Connect field to convert
    * @return The equivalent Kudu type
    **/
  def convertConnectField(field: Field): ColumnSchema = {
    val fieldType = field.schema().`type`()
    val fieldName = field.name()
    val kudu = fieldType match {
      case Type.STRING => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.STRING)
      case Type.INT8 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT8)
      case Type.INT16 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT16)
      case Type.INT32 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT32)
      case Type.INT64 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT64)
      case Type.BOOLEAN => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.BOOL)
      case Type.FLOAT32 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.FLOAT)
      case Type.FLOAT64 => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.DOUBLE)
      case Type.BYTES => new ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.BINARY)
      case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
    }
    val default = field.schema().defaultValue()
    if (default != null) kudu.defaultValue(default)
    kudu.build()
  }

  /**
    * Convert an Avro schema
    *
    * @param schema    The avro field schema to convert
    * @param fieldName The fieldName to use for the Kudu column
    * @return A Kudu ColumnSchemaBuilder
    **/
  def fromAvro(schema: org.apache.avro.Schema, fieldName: String): ColumnSchemaBuilder = {
    schema.getType match {
      case org.apache.avro.Schema.Type.RECORD =>
        throw new RuntimeException("Avro type RECORD not supported")
      case org.apache.avro.Schema.Type.ARRAY => throw new RuntimeException("Avro type ARRAY not supported")
      case org.apache.avro.Schema.Type.MAP => throw new RuntimeException("Avro type MAP not supported")
      case org.apache.avro.Schema.Type.UNION =>
        val union = getNonNull(schema)
        fromAvro(union, fieldName)
      case org.apache.avro.Schema.Type.FIXED => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.BINARY)
      case org.apache.avro.Schema.Type.STRING => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.STRING)
      case org.apache.avro.Schema.Type.BYTES => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.BINARY)
      case org.apache.avro.Schema.Type.INT => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT32)
      case org.apache.avro.Schema.Type.LONG => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.INT64)
      case org.apache.avro.Schema.Type.FLOAT => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.FLOAT)
      case org.apache.avro.Schema.Type.DOUBLE => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.DOUBLE)
      case org.apache.avro.Schema.Type.BOOLEAN => new ColumnSchema.ColumnSchemaBuilder(fieldName, org.apache.kudu.Type.BOOL)
      case org.apache.avro.Schema.Type.NULL => throw new RuntimeException("Avro type NULL not supported")
      case _ => throw new RuntimeException("Avro type not supported")
    }
  }

  /**
    * Resolve unions
    *
    * @param schema The schema to resolve the union for
    * @return An Avro schema for the data type from the union
    **/
  private def getNonNull(schema: org.apache.avro.Schema): org.apache.avro.Schema = {
    val unionTypes = schema.getTypes
    if (unionTypes.size == 2) {
      if (unionTypes.get(0).getType == org.apache.avro.Schema.Type.NULL) {
        unionTypes.get(1)
      }
      else if (unionTypes.get(1).getType == org.apache.avro.Schema.Type.NULL) {
        unionTypes.get(0)
      }
      else {
        schema
      }
    }
    else {
      schema
    }
  }
}


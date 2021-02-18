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

package com.datamountaineer.streamreactor.connect.bloomberg.avro

import java.io.ByteArrayOutputStream

import com.datamountaineer.streamreactor.connect.bloomberg.BloombergData
import com.datamountaineer.streamreactor.connect.bloomberg.avro.AvroSchemaGenerator._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory

import scala.collection.JavaConverters._

object AvroSerializer {

  /**
    * Provides the method to serialize a BloombergData instance to avro.
    * Only the data field is taken into account
    *
    * @param data Bloomberg data to serialize to avro
    */
  implicit class BloombergDataToAvroSerialization(val data: BloombergData) {
    def toAvro: Array[Byte] = {
      val schema = data.getSchema

      val output = new ByteArrayOutputStream()
      val writer = new GenericDatumWriter[GenericRecord](schema)
      val encoder = EncoderFactory.get().binaryEncoder(output, null)

      val record = data.data.toAvroRecord(schema)
      writer.write(record, encoder)
      encoder.flush()
      output.flush()
      output.toByteArray
    }
  }


  /**
    * Converts a map to an avro generic record to be serialized as avro
    *
    * @param map A Map to convert to a generic record
    */
  implicit class MapToGenericRecordConverter(val map: java.util.Map[String, Any]) {
    def toAvroRecord(schema: Schema): GenericData.Record = {
      val record = new Record(schema)
      map.entrySet().asScala.foreach { e => recursive(record, schema, e.getKey, e.getValue) }
      record
    }

    /**
      * Given the instance of the value will either add the value to the generic record or create another
      *
      * @param record A GenericRecord to add the value to
      * @param schema The schema for the field
      * @param fieldName The field name
      * @param value The value of the field
      */
    private def recursive(record: GenericData.Record, schema: Schema, fieldName: String, value: Any): Unit = {
      value match {
        case _: Boolean => record.put(fieldName, value)
        case _: Int => record.put(fieldName, value)
        case _: Long => record.put(fieldName, value)
        case _: Double => record.put(fieldName, value)
        case _: Char => record.put(fieldName, value)
        case _: Float => record.put(fieldName, value)
        case _: String =>
          record.put(fieldName, value)
        case list: java.util.List[_] =>
          val tmpSchema = schema.getField(fieldName).schema()
          val itemSchema = if (tmpSchema.getType == Schema.Type.UNION) tmpSchema.getTypes.get(1) else tmpSchema
          require(itemSchema.getType == Schema.Type.ARRAY)
          //we might have a record not a primitive
          if (itemSchema.getElementType.getType == Schema.Type.RECORD) {
            val items = new GenericData.Array[GenericData.Record](list.size(), itemSchema)
            list.asScala.foreach { i =>
              //only map is allowed
              val m = i.asInstanceOf[java.util.Map[String, Any]]
              items.add(m.toAvroRecord(itemSchema.getElementType))
            }
            record.put(fieldName, items)
          } else {
            val items = new GenericData.Array[Any](list.size(), itemSchema)
            items.addAll(list)
            record.put(fieldName, items)
          }

        case map: java.util.LinkedHashMap[String @unchecked, _] =>
          //record schema
          val fieldSchema = schema.getField(fieldName).schema()
          val nestedSchema = if (fieldSchema.getType == Schema.Type.UNION) fieldSchema.getTypes.get(1) else fieldSchema
          val nestedRecord = new Record(nestedSchema)
          map.entrySet().asScala.foreach(e =>
            recursive(nestedRecord, nestedSchema, e.getKey, e.getValue))
          record.put(fieldName, nestedRecord)
      }
    }
  }
}

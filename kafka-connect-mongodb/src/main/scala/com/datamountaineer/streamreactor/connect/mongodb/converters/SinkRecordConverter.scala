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

package com.datamountaineer.streamreactor.connect.mongodb.converters

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone

import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.json4s.JValue
import org.json4s.JsonAST._

import scala.collection.JavaConversions._

object SinkRecordConverter {
  private val ISO_DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

  /**
    * Creates a Mongo document from a HashMap
    *
    * @param map
    * @return
    */
  def fromMap(map: util.HashMap[String, AnyRef]): Document = new Document(map)

  /**
    * Creates a Mongo document from a the Kafka Struct
    *
    * @param record
    * @return
    */
  def fromStruct(record: SinkRecord): Document = {

    def convertToDocument(schema: Schema, value: Any): Any = {
      Option(value) match {
        case None =>
          if (schema == null) null
          else {
            if (schema.defaultValue != null) convertToDocument(schema, schema.defaultValue)
            else if (schema.isOptional) null
            else throw new DataException("Conversion error: null value for field that is required and has no default value")
          }

        case Some(_) =>

          try {
            val schemaType = Option(schema).map(_.`type`())
              .orElse(Option(ConnectSchema.schemaType(value.getClass)))
              .getOrElse(throw new DataException("Class " + value.getClass + " does not have corresponding schema type."))

            schemaType match {

              case Schema.Type.INT8 | Schema.Type.INT16 | Schema.Type.BOOLEAN | Schema.Type.FLOAT32 | Schema.Type.FLOAT64 => value
              case Schema.Type.INT32 =>
                if (schema != null && Date.LOGICAL_NAME == schema.name) ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                else if (schema != null && Time.LOGICAL_NAME == schema.name) TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                else value

              case Schema.Type.INT64 =>
                if (schema != null && Timestamp.LOGICAL_NAME == schema.name) Timestamp.toLogical(schema, value.asInstanceOf[Long])
                else value

              case Schema.Type.STRING => value.asInstanceOf[CharSequence].toString

              case Schema.Type.BYTES =>
                if (schema != null && Decimal.LOGICAL_NAME == schema.name) Decimal.toLogical(schema, value.asInstanceOf[Array[Byte]])
                else value match {
                  case arrayByte: Array[Byte] => arrayByte
                  case buffer: ByteBuffer => buffer.array
                  case _ => throw new DataException("Invalid type for bytes type: " + value.getClass)
                }

              case Schema.Type.ARRAY =>
                val valueSchema = Option(schema).map(_.valueSchema()).orNull
                val list = new java.util.ArrayList[Any]
                value
                  .asInstanceOf[java.util.Collection[_]]
                  .foreach { elem =>
                    list.add(convertToDocument(valueSchema, elem))
                  }
                list

              case Schema.Type.MAP =>
                val map = value.asInstanceOf[java.util.Map[_, _]]
                // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                val objectMode: Boolean = Option(schema)
                  .map(_.keySchema.`type` eq Schema.Type.STRING)
                  .getOrElse(map.entrySet().headOption.forall(_.getKey.isInstanceOf[String]))

                var obj: Document = null
                var list: java.util.ArrayList[Any] = null

                if (objectMode) obj = new Document()
                else list = new util.ArrayList[Any]()

                for (entry <- map.entrySet) {
                  val keySchema = Option(schema).map(_.keySchema()).orNull
                  val valueSchema = Option(schema).map(_.valueSchema).orNull

                  val mapKey = convertToDocument(keySchema, entry.getKey)
                  //don't add null
                  Option(convertToDocument(valueSchema, entry.getValue))
                    .foreach { mapValue =>
                      if (objectMode) {
                        obj.append(mapKey.toString, mapValue)
                      }
                      else {
                        val innerArray = new util.ArrayList[Any]()
                        innerArray.add(mapKey)
                        innerArray.add(mapValue)
                        list.add(innerArray)
                      }
                    }
                }
                if (objectMode) obj
                else list

              case Schema.Type.STRUCT =>
                val struct = value.asInstanceOf[Struct]
                if (struct.schema != schema) throw new DataException("Mismatching schema.")

                schema.fields
                  .foldLeft(new Document) { (document, field) =>
                    Option(convertToDocument(field.schema, struct.get(field)))
                      .foreach(//case bd: BigDecimal => document.append(field.name, bd.toDouble)
                        //case bi: BigInt => document.append(field.name(), bi.toLong)
                        v => document.append(field.name, v))
                    document
                  }

              case other =>
                schema.name() match {
                  case Decimal.LOGICAL_NAME => Decimal.toLogical(schema, value.asInstanceOf[Array[Byte]])
                  case Date.LOGICAL_NAME => ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                  case Time.LOGICAL_NAME => TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                  case Timestamp.LOGICAL_NAME => ISO_DATE_FORMAT.format(Timestamp.toLogical(schema, value.asInstanceOf[Long]))
                  case _ => sys.error(s"$other is not a recognized schema")
                }
            }
          }
          catch {
            case _: ClassCastException => throw new DataException("Invalid type for " + schema.`type` + ": " + value.getClass)
          }
      }
    }

    if (!record.value().isInstanceOf[Struct]) {
      throw new IllegalArgumentException(s"Expecting a Struct. ${record.value.getClass}")
    }
    //we let it fail in case of a non struct(shouldn't be the case)
    convertToDocument(record.valueSchema(), record.value()).asInstanceOf[Document]
  }

  /**
    * Creates a Mongo document from Json
    *
    * @param record - The instance to the json node
    * @return An instance of a mongo document
    */
  def fromJson(record: JValue): Document = {
    def convert(name: String, jvalue: JValue, document: Document): Document = {
      def convertArray(array: JArray): util.ArrayList[Any] = {
        val list = new util.ArrayList[Any]()
        array.children.filter {
          case JNull | JNothing => false
          case _ => true
        }.map {
          case JObject(values) => values.foldLeft(new Document) { case (d, (n, j)) => convert(n, j, d) }
          case JBool(b) => b
          case JDecimal(d) => d.toDouble //need to do this because of mong not understanding the codec
          case JDouble(d) => d
          case JInt(i) => i.toLong //need to do this because of mongo
          case JLong(l) => l
          case JString(s) => s
          case JNothing => null
          case JNull => null
          case arr: JArray => convertArray(arr)
        }.foreach(list.add)
        list
      }

      val value = jvalue match {
        case arr: JArray => convertArray(arr)
        case JBool(b) => b
        case JDecimal(d) => d.toDouble //need to do this because of mong
        case JDouble(d) => d
        case JInt(i) => i.toLong //need to do this because of mongo
        case JLong(l) => l
        case JNothing => null
        case JNull => null
        case JString(s) => s
        case JObject(values) => values.foldLeft(new Document) { case (d, (n, j)) => convert(n, j, d) }
      }
      Option(value).map(document.append(name, _)).getOrElse(document)
    }

    record match {
      case jobj: JObject =>
        jobj.obj.foldLeft(new Document) { case (d, JField(n, j)) => convert(n, j, d) }
      case _ => throw new IllegalArgumentException("Invalid json to convert to mongo ")
    }
  }
}

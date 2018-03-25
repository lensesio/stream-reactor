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
package com.datamountaineer.streamreactor.connect.azure.documentdb.converters

import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.TimeZone

import com.datamountaineer.streamreactor.connect.azure.documentdb.Json
import com.microsoft.azure.documentdb.Document
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.JValue
import org.json4s.JsonAST._

import scala.collection.JavaConversions._

object SinkRecordConverter {
  private val ISO_DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

  /**
    * Creates a Azure Document Db document from a HashMap
    *
    * @param map
    * @return
    */
  def fromMap(map: util.HashMap[String, AnyRef]): Document = new Document(Json.toJson(map))

  /**
    * Creates an Azure DocumentDb document from a the Kafka Struct
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
              case Schema.Type.INT32 =>
                if (schema != null && Date.LOGICAL_NAME == schema.name) ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                else if (schema != null && Time.LOGICAL_NAME == schema.name) TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                else value

              case Schema.Type.INT64 =>
                if (Timestamp.LOGICAL_NAME == schema.name) Timestamp.fromLogical(schema, value.asInstanceOf[(java.util.Date)])
                else value

              case Schema.Type.STRING => value.asInstanceOf[CharSequence].toString

              case Schema.Type.BYTES =>
                if (Decimal.LOGICAL_NAME == schema.name) value.asInstanceOf[BigDecimal]
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
                var objectMode: Boolean = Option(schema)
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
                        obj.set(mapKey.toString, mapValue)
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
                      .foreach {
                        //case bd: BigDecimal => document.append(field.name, bd.toDouble)
                        //case bi: BigInt => document.append(field.name(), bi.toLong)
                        v => document.set(field.name, v)
                      }
                    document
                  }

              case _ => value
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
    * Creates an Azure Document DB document from Json
    *
    * @param record - The instance to the json node
    * @return An instance of a Azure Document DB document
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
          case JDecimal(d) => d.toDouble
          case JDouble(d) => d
          case JInt(i) => i.toLong
          case JLong(l) => l
          case JString(s) => s
          case JNull | JNothing => ""
          case arr: JArray => convertArray(arr)
        }.foreach(list.add)
        list
      }

      val value = jvalue match {
        case arr: JArray => convertArray(arr)
        case JBool(b) => b
        case JDecimal(d) => d.toDouble
        case JDouble(d) => d
        case JInt(i) => i.toLong
        case JLong(l) => l
        case JNothing => null
        case JNull => null
        case JString(s) => s
        case JObject(values) => values.foldLeft(new Document) { case (d, (n, j)) => convert(n, j, d) }
      }
      Option(value).map { v =>
        document.set(name, v)
        document
      }.getOrElse(document)
    }

    record match {
      case jobj: JObject =>
        jobj.obj.foldLeft(new Document) { case (d, JField(n, j)) => convert(n, j, d) }
      case _ => throw new IllegalArgumentException("Can't convert invalid json!")
    }
  }
}

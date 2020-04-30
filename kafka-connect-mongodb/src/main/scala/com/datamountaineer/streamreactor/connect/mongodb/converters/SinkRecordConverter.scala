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
import java.time.OffsetDateTime
import java.util
import java.util.TimeZone

import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.json4s.JValue
import org.json4s.JsonAST._

import scala.collection.JavaConverters._
import scala.util.Try

object SinkRecordConverter extends StrictLogging {
  private val ISO_DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

  /**
    * Creates a Mongo document from a HashMap
    *
    * @param map
    * @return
    */
  def fromMap(map: util.Map[String, AnyRef])(implicit settings: MongoSettings):
    Document = {

    val doc = new Document(map)

    // mutate the doc if requested before returning
    if (settings.jsonDateTimeFields.nonEmpty) SinkRecordConverter.convertTimestamps(doc)
    doc
  }

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
                if(schema != null) {
                  schema.name() match {
                    case Date.LOGICAL_NAME =>
                      value match {
                        case d: java.util.Date => ISO_DATE_FORMAT.format(d)
                        case _ => ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                      }

                    case Time.LOGICAL_NAME =>
                      value match {
                        case d: java.util.Date => TIME_FORMAT.format(d)
                        case _ => TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                      }

                    case _=> value
                  }
                } else value

              case Schema.Type.INT64 =>
                if (schema != null) {
                  schema.name match {
                    case Timestamp.LOGICAL_NAME =>
                      value match {
                        case d: java.util.Date => ISO_DATE_FORMAT.format(d)
                        case _ => ISO_DATE_FORMAT.format(Timestamp.toLogical(schema, value.asInstanceOf[Long]))
                      }
                    case _ => value
                  }
                }
                else value

              case Schema.Type.STRING => value.asInstanceOf[CharSequence].toString

              case Schema.Type.BYTES =>
                if (schema != null && Decimal.LOGICAL_NAME == schema.name) {
                  value match {
                    case jbd: java.math.BigDecimal => jbd
                    case bd: BigDecimal => bd.bigDecimal
                    case bb: ByteBuffer => Decimal.toLogical(schema, bb.array())
                    case _ => Decimal.toLogical(schema, value.asInstanceOf[Array[Byte]])
                  }
                }
                else value match {
                  case arrayByte: Array[Byte] => arrayByte
                  case buffer: ByteBuffer => buffer.array
                  case _ => throw new DataException("Invalid type for bytes type: " + value.getClass)
                }

              case Schema.Type.ARRAY =>
                val valueSchema = Option(schema).map(_.valueSchema()).orNull
                val list = new java.util.ArrayList[Any]
                value
                  .asInstanceOf[java.util.Collection[_]].asScala
                  .foreach { elem =>
                    list.add(convertToDocument(valueSchema, elem))
                  }
                list

              case Schema.Type.MAP =>
                val map = value.asInstanceOf[java.util.Map[_, _]]
                // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                val objectMode: Boolean = Option(schema)
                  .map(_.keySchema.`type` eq Schema.Type.STRING)
                  .getOrElse(map.entrySet().asScala.headOption.forall(_.getKey.isInstanceOf[String]))

                var obj: Document = null
                var list: java.util.ArrayList[Any] = null

                if (objectMode) obj = new Document()
                else list = new util.ArrayList[Any]()

                for (entry <- map.entrySet.asScala) {
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

                schema.fields.asScala
                  .foldLeft(new Document) { (document, field) =>
                    Option(convertToDocument(field.schema, struct.get(field)))
                      .foreach(//case bd: BigDecimal => document.append(field.name, bd.toDouble)
                        //case bi: BigInt => document.append(field.name(), bi.toLong)
                        v => document.append(field.name, v))
                    document
                  }

              case other =>
                schema.name() match {
                  case Decimal.LOGICAL_NAME =>
                    value match {
                      case bd: BigDecimal => bd.bigDecimal
                      case bb: ByteBuffer => Decimal.toLogical(schema, bb.array())
                      case _ => Decimal.toLogical(schema, value.asInstanceOf[Array[Byte]])
                    }
                  case Date.LOGICAL_NAME =>
                    value match {
                      case d: java.util.Date => ISO_DATE_FORMAT.format(d)
                      case _ => ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                    }

                  case Time.LOGICAL_NAME =>
                    value match {
                      case d: java.util.Date => TIME_FORMAT.format(d)
                      case _ => TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                    }

                  case Timestamp.LOGICAL_NAME =>
                    value match {
                      case d: java.util.Date => ISO_DATE_FORMAT.format(d)
                      case _ => ISO_DATE_FORMAT.format(Timestamp.toLogical(schema, value.asInstanceOf[Long]))
                    }

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
  def fromJson(record: JValue)(implicit settings: MongoSettings): Document = {
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

    val doc = record match {
      case jobj: JObject =>
        jobj.obj.foldLeft(new Document) { case (d, JField(n, j)) => convert(n, j, d) }
      case _ => throw new IllegalArgumentException("Invalid json to convert to mongo ")
    }

    // mutate the doc if requested before returning
    if (settings.jsonDateTimeFields.nonEmpty) convertTimestamps(doc)
    doc
  }

  /**
    * Convert timestamps based on settings' jsonDateTimeFields.
    * @param doc
    * @return Unit - the input document is modified in-place!
    */
  def convertTimestamps(doc: Document)(implicit settings: MongoSettings): Unit = {

    import scala.collection.JavaConverters._
    val fieldSet: Set[Seq[String]] = settings.jsonDateTimeFields

    logger.debug(s"convertTimestamps: converting document ${doc.toString}")
    logger.debug(s"convertTimestamps: using jsonDateTimeFields of ${settings.jsonDateTimeFields}")

    val initialDoc = new Document()
    fieldSet.foreach{ parts =>

      def convertValue(
        remainingParts: Seq[String],
        lastDoc: java.util.Map[String, Object]): Unit = {

        val head = remainingParts.headOption
        remainingParts.size match {
          case 1 => {
            val testVal = lastDoc.get(head.get)
            val newVal: Option[Object] = testVal match {
              case s: String => Option {
                Try( OffsetDateTime.parse(s).toInstant().toEpochMilli() ).
                  toOption.
                  map { millis => new java.util.Date(millis) }.
                  getOrElse(s)
              }
              case i: Integer => Option(new java.util.Date(i.longValue()))
              case i: java.lang.Long => Option(new java.util.Date(i))
              case _ => None
            }
            newVal.map{ nv => lastDoc.put(head.get, nv) }
          }
          case n: Int if (n > 1) => {
            val testVal = lastDoc.get(head.get)
            testVal match {
              case subDoc: java.util.Map[String, Object] => // Document implements Map, HashMap is used sometimes too for subdocs
                convertValue(remainingParts.tail, subDoc)
              case subList: java.util.List[_] => {
                subList.asScala.foreach { listDoc =>
                  listDoc match {
                    case d: java.util.Map[String, Object] =>
                      convertValue(remainingParts.tail, d)
                    case _ => // not a document, can't determine the name, do nothing
                  }
                }
              }
              case _ => // not a list or doc, can't do anything
            }
          }
          case _ => throw new Exception("somehow remainingParts is 0!")
        }
      }
      convertValue(parts, doc)
    }

    logger.debug("converted doc is: "+doc.toString)
  }
}

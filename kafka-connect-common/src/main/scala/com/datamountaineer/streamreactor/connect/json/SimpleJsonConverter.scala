/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.json

import cats.implicits.catsSyntaxOptionId
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.ConnectSchema
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Date => ConnectDate }
import org.apache.kafka.connect.data.{ Decimal => ConnectDecimal }
import org.apache.kafka.connect.data.{ Time => ConnectTime }
import org.apache.kafka.connect.data.{ Timestamp => ConnectTimestamp }
import org.apache.kafka.connect.errors.DataException

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util
import java.util.{ Date => JavaDate }
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
  * Implementation of Converter that uses JSON to store schemas and objects.
  */
object SimpleJsonConverter {

  private val ISO_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-d'T'HH:mm:ss.SSS'Z'").withZone(ZoneId.of("UTC"))
  private val TIME_FORMAT     = DateTimeFormatter.ofPattern("HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault())

  /**
    * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
    * and the converted object.
    */
  private def convertToJson(schema: Schema, logicalValue: Any): JsonNode =
    convertToJsonInner(Option(schema), Option(logicalValue)).orNull

  private def convertToJsonInner(maybeSchema: Option[Schema], maybeLogicalValue: Option[Any]): Option[JsonNode] =
    maybeLogicalValue match {
      case Some(logicalValue) => try {
          extractSchemaType(logicalValue, maybeSchema) match {
            case INT8 =>
              JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Byte]).some
            case INT16 =>
              JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Short]).some
            case INT32 =>
              if (maybeSchema.exists(_.name == ConnectDate.LOGICAL_NAME)) {
                JsonNodeFactory.instance.textNode(
                  ISO_DATE_FORMAT.format(logicalValue.asInstanceOf[JavaDate].toInstant),
                ).some
              } else if (maybeSchema.exists(_.name == ConnectTime.LOGICAL_NAME)) {
                JsonNodeFactory.instance.textNode(
                  TIME_FORMAT.format(logicalValue.asInstanceOf[JavaDate].toInstant),
                ).some
              } else {
                JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Integer]).some
              }
            case INT64 =>
              if (maybeSchema.exists(_.name == ConnectTimestamp.LOGICAL_NAME)) {
                JsonNodeFactory.instance.numberNode(ConnectTimestamp.fromLogical(maybeSchema.get,
                                                                                 logicalValue.asInstanceOf[JavaDate],
                )).some
              } else {
                JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Long]).some
              }
            case FLOAT32 =>
              JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Float]).some
            case FLOAT64 =>
              JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[Double]).some
            case BOOLEAN =>
              JsonNodeFactory.instance.booleanNode(logicalValue.asInstanceOf[Boolean]).some
            case STRING =>
              val charSeq = logicalValue.asInstanceOf[CharSequence]
              JsonNodeFactory.instance.textNode(charSeq.toString).some
            case BYTES =>
              if (maybeSchema.exists(_.name() == ConnectDecimal.LOGICAL_NAME)) {
                JsonNodeFactory.instance.numberNode(logicalValue.asInstanceOf[BigDecimal]).some
              } else {
                val valueArr: Array[Byte] = logicalValue match {
                  case bytes:  Array[Byte] => bytes
                  case buffer: ByteBuffer  => buffer.array
                  case _ => null
                }
                if (valueArr == null) throw new DataException("Invalid type for bytes type: " + logicalValue.getClass)
                JsonNodeFactory.instance.binaryNode(valueArr).some
              }
            case ARRAY =>
              val collection = logicalValue.asInstanceOf[util.Collection[AnyRef]].asScala
              val list       = JsonNodeFactory.instance.arrayNode
              for (elem <- collection) {
                list.add(schemaToJson(maybeSchema, elem, _.valueSchema()))
              }
              list.some

            case MAP =>
              val map = logicalValue.asInstanceOf[util.Map[AnyRef, AnyRef]].asScala.toMap
              // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
              val simpleNode = SimpleJsonNode(map, maybeSchema)
              for ((k, v) <- map) {
                simpleNode.addToNode(
                  schemaToJson(maybeSchema, k, _.keySchema()),
                  schemaToJson(maybeSchema, v, _.valueSchema()),
                )
              }
              simpleNode.get().some

            case STRUCT =>
              val struct = logicalValue.asInstanceOf[Struct]
              maybeSchema match {
                case Some(sch) if sch != struct.schema() =>
                  throw new DataException("Mismatching schema.")
                case Some(sch) =>
                  val obj: ObjectNode = JsonNodeFactory.instance.objectNode()
                  sch.fields().forEach {
                    field =>
                      obj.set(field.name(), convertToJson(field.schema, struct.get(field)))
                  }
                  obj.some
                case None => throw new DataException("Missing schema.")
              }

            case _ =>
              throw new DataException(s"Couldn't convert $logicalValue to JSON.")
          }
        } catch {
          case e: ClassCastException =>
            throw new DataException("Invalid type for " + maybeSchema.map(_.`type`) + ": " + logicalValue.getClass, e)
        }
      case None =>
        maybeSchema match {
          case None => Option.empty
          case Some(schema) => if (schema.defaultValue != null) return Some(convertToJson(schema, schema.defaultValue))
            if (schema.isOptional) return Some(JsonNodeFactory.instance.nullNode)
            throw new DataException("Conversion error: null value for field that is required and has no default value")
        }
    }

  private def schemaToJson(maybeSchema: Option[Schema], elem: AnyRef, schemaFn: Schema => Schema): JsonNode = {
    val valueSchema = maybeSchema.map(schemaFn).orNull
    convertToJson(valueSchema, elem)
  }

  private def extractSchemaType(logicalValue: Any, maybeSchema: Option[Schema]): Schema.Type =
    maybeSchema match {
      case Some(schema) => schema.`type`
      case None =>
        val schemaType = ConnectSchema.schemaType(logicalValue.getClass)
        if (schemaType == null)
          throw new DataException("Java class " + logicalValue.getClass + " does not have corresponding schema type.")
        schemaType
    }
}
class SimpleJsonConverter {
  def fromConnectData(schema: Schema, value: Any): JsonNode = SimpleJsonConverter.convertToJson(schema, value)
}

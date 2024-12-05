/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic6

import cats.implicits.catsSyntaxEitherId
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.BigIntegerNode
import com.fasterxml.jackson.databind.node.BooleanNode
import com.fasterxml.jackson.databind.node.DecimalNode
import com.fasterxml.jackson.databind.node.DoubleNode
import com.fasterxml.jackson.databind.node.FloatNode
import com.fasterxml.jackson.databind.node.IntNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.TextNode
import io.lenses.json.sql.JacksonJson
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object JsonPayloadExtractor {
  lazy val simpleJsonConverter = new SimpleJsonConverter()

  def extractJsonNode(value: Any, schema: Schema): Either[String, Option[JsonNode]] =
    (Option(value), Option(schema).map(_.`type`())) match {
      case (None, _)                            => Right(None)
      case (Some(_), Some(Schema.Type.BYTES))   => handleBytes(value)
      case (Some(_), Some(Schema.Type.STRING))  => handleString(value)
      case (Some(_), Some(Schema.Type.INT8))    => handleLong(value)
      case (Some(_), Some(Schema.Type.INT16))   => handleLong(value)
      case (Some(_), Some(Schema.Type.INT32))   => handleLong(value)
      case (Some(_), Some(Schema.Type.INT64))   => handleLong(value)
      case (Some(_), Some(Schema.Type.FLOAT32)) => handleFloat(value)
      case (Some(_), Some(Schema.Type.FLOAT64)) => handleDouble(value)
      case (Some(_), Some(Schema.Type.STRUCT))  => handleStruct(value)
      case (Some(_), Some(Schema.Type.BOOLEAN)) => handleBoolean(value)
      case (Some(_), Some(Schema.Type.ARRAY))   => handleArray(value)
      case (Some(_), Some(Schema.Type.MAP))     => handleMap(value)
      case (Some(_), Some(other))               => Left(s"Unsupported Schema type: $other")
      case (Some(v), None)                      => handleSchemaLess(v)
    }

  private def handleArray(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case l: Iterable[_] =>
        val arrayNode = JacksonJson.mapper.createArrayNode()
        l.foreach { item =>
          extractJsonNode(item, null) match {
            case Right(Some(node)) => arrayNode.add(node)
            case Right(None)       => // ignore
            case Left(err)         => return Left(err)
          }
        }
        Right(Some(arrayNode))

      case jc: java.util.Collection[_] =>
        val arrayNode = JacksonJson.mapper.createArrayNode()
        jc.asScala.foreach { item =>
          extractJsonNode(item, null) match {
            case Right(Some(node)) => arrayNode.add(node)
            case Right(None)       => // ignore
            case Left(err)         => return Left(err)
          }
        }
        Right(Some(arrayNode))
      case a: Array[_] =>
        val arrayNode = JacksonJson.mapper.createArrayNode()
        a.foreach { item =>
          extractJsonNode(item, null) match {
            case Right(Some(node)) => arrayNode.add(node)
            case Right(None)       => // ignore
            case Left(err)         => return Left(err)
          }
        }
        Right(Some(arrayNode))
      case other => Left(s"Expected array but got: $other")
    }

  private def handleMap(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case m: java.util.Map[_, _] =>
        val map     = m.asInstanceOf[java.util.Map[String, Any]]
        val mapNode = JacksonJson.mapper.createObjectNode()
        map.asScala.foreach {
          case (key, value) =>
            extractJsonNode(value, null) match {
              case Right(Some(node)) => mapNode.set(key, node)
              case Right(None)       => // ignore
              case Left(err)         => return Left(err)
            }
        }
        Right(Some(mapNode))
      case other => Left(s"Expected map but got: $other")
    }
  private def handleBoolean(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case b: Boolean => Some(BooleanNode.valueOf(b)).asRight[String]
      case other => Left(s"Expected boolean but got: $other")
    }
  private def handleDouble(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case f: Float  => Some(DoubleNode.valueOf(f.toDouble)).asRight[String]
      case d: Double => Some(DoubleNode.valueOf(d)).asRight[String]
      case other => Left(s"Expected double but got: $other")
    }

  private def handleFloat(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case f: Float  => Some(FloatNode.valueOf(f)).asRight[String]
      case d: Double => Some(FloatNode.valueOf(d.toFloat)).asRight[String]
      case other => Left(s"Expected float but got: $other")
    }

  private def handleLong(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case b: Byte  => Some(LongNode.valueOf(b.toLong)).asRight[String]
      case s: Short => Some(LongNode.valueOf(s.toLong)).asRight[String]
      case i: Int   => Some(LongNode.valueOf(i.toLong)).asRight[String]
      case l: Long  => Some(LongNode.valueOf(l)).asRight[String]
      case other => Left(s"Expected long but got: $other")
    }

  private def handleBytes(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case bytes: Array[Byte] =>
        tryReadJson(bytes).map(Some(_))
      case byteBuffer: ByteBuffer =>
        val bytes = new Array[Byte](byteBuffer.remaining())
        byteBuffer.get(bytes)
        tryReadJson(bytes).map(Some(_))
      case other => Left(s"Expected byte array or ByteBuffer but got: $other")
    }

  private def handleString(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case s: String =>
        tryParseJson(s).map(Some(_)) match {
          case Left(_) => TextNode.valueOf(s).asRight[String].map(Some(_))
          case r       => r
        }
      case other => Left(s"Expected string but got: $other")
    }

  private def handleStruct(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case struct: Struct =>
        Try(simpleJsonConverter.fromConnectData(struct.schema(), struct)) match {
          case Success(jsonNode) => Right(Some(jsonNode))
          case Failure(e)        => Left(s"Failed to convert Struct to JsonNode: ${e.getMessage}")
        }
      case other => Left(s"Expected Struct but got: $other")
    }

  private def handleSchemaLess(value: Any): Either[String, Option[JsonNode]] =
    value match {
      case m: java.util.Map[_, _] =>
        Try {
          val map = m.asInstanceOf[java.util.Map[String, Any]]
          JacksonJson.mapper.valueToTree[JsonNode](map)
        } match {
          case Success(node) => Right(Some(node))
          case Failure(e)    => Left(s"Failed to convert Map to JsonNode: ${e.getMessage}")
        }

      case s: String =>
        tryParseJson(s).map(Some(_)) match {
          case Left(_) => TextNode.valueOf(s).asRight[String].map(Some(_))
          case r       => r
        }
      case b:          Array[Byte]          => tryReadJson(b).map(Some(_))
      case b:          Byte                 => IntNode.valueOf(b.toInt).asRight[String].map(Some(_))
      case s:          Short                => IntNode.valueOf(s.toInt).asRight[String].map(Some(_))
      case i:          Int                  => IntNode.valueOf(i).asRight[String].map(Some(_))
      case l:          Long                 => LongNode.valueOf(l).asRight[String].map(Some(_))
      case f:          Float                => FloatNode.valueOf(f).asRight[String].map(Some(_))
      case double:     Double               => DoubleNode.valueOf(double).asRight[String].map(Some(_))
      case bigDecimal: BigDecimal           => DecimalNode.valueOf(bigDecimal.bigDecimal).asRight[String].map(Some(_))
      case bigDecimal: java.math.BigDecimal => DecimalNode.valueOf(bigDecimal).asRight[String].map(Some(_))
      case boolean:    Boolean              => BooleanNode.valueOf(boolean).asRight[String].map(Some(_))
      case bi:         BigInt               => BigIntegerNode.valueOf(bi.bigInteger).asRight[String].map(Some(_))
      case bi:         java.math.BigInteger => BigIntegerNode.valueOf(bi).asRight[String].map(Some(_))
      case other => Left(s"Unsupported value type: ${other.getClass.getName}")
    }

  private def tryParseJson(str: String): Either[String, JsonNode] =
    Try(JacksonJson.asJson(str)) match {
      case Success(json) => Right(json)
      case Failure(e)    => Left(s"Invalid JSON string: ${e.getMessage}")
    }

  private def tryReadJson(bytes: Array[Byte]): Either[String, JsonNode] =
    Try(JacksonJson.mapper.readTree(bytes)) match {
      case Success(json) => Right(json)
      case Failure(e)    => Left(s"Invalid JSON bytes: ${e.getMessage}")
    }
}

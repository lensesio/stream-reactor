package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.nio.ByteBuffer

import com.datamountaineer.streamreactor.connect.json.SimpleJsonConverter
import com.fasterxml.jackson.databind.JsonNode
import com.landoop.connect.sql.StructSql._
import com.landoop.json.sql.JsonSql._
import com.landoop.json.sql.{Field, JacksonJson}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.util.{Failure, Success, Try}

private object Transform extends StrictLogging {
  lazy val simpleJsonConverter = new SimpleJsonConverter()

  def apply(fields: Seq[Field],
            ignoredFields: Seq[Field],
            schema: Schema,
            value: Any,
            withStructure:Boolean): String = {
    def raiseException(msg: String, t: Throwable) = throw new IllegalArgumentException(msg, t)

    if (value == null) {
      if (schema == null || !schema.isOptional) {
        raiseException("Null value is not allowed.", null)
      }
      else null
    } else {
      if (schema != null) {
        schema.`type`() match {
          case Schema.Type.BYTES =>
            //we expected to be json
            val array = value match {
              case a: Array[Byte] => a
              case b: ByteBuffer => b.array()
              case other => raiseException("Invalid payload:$other for schema Schema.BYTES.", null)
            }

            Try(JacksonJson.mapper.readTree(array)) match {
              case Failure(e) => raiseException("Invalid json.", e)
              case Success(json) =>
                Try(json.sql(fields, !withStructure)) match {
                  case Failure(e) => raiseException(s"A KCQL exception occurred. ${e.getMessage}", e)
                  case Success(jn) => jn.toString
                }
            }

          case Schema.Type.STRING =>
            //we expected to be json
            Try(JacksonJson.asJson(value.asInstanceOf[String])) match {
              case Failure(e) => raiseException("Invalid json", e)
              case Success(json) =>
                Try(json.sql(fields, !withStructure)) match {
                  case Success(jn) => jn.toString
                  case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", e)
                }
            }

          case Schema.Type.STRUCT =>
            val struct = value.asInstanceOf[Struct]
            Try(struct.sql(fields, !withStructure)) match {
              case Success(s) => simpleJsonConverter.fromConnectData(s.schema(), s).toString

              case Failure(e) => raiseException(s"A KCQL error occurred.${e.getMessage}", e)
            }

          case other => raiseException("Can't transform Schema type:$other.", null)
        }
      } else {
        //we can handle java.util.Map (this is what JsonConverter can spit out)
        value match {
          case m: java.util.Map[_, _] =>
            val map = m.asInstanceOf[java.util.Map[String, Any]]
            val jsonNode: JsonNode = JacksonJson.mapper.valueToTree(map)
            Try(jsonNode.sql(fields, !withStructure)) match {
              case Success(j) => j.toString
              case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", e)
            }
          case s: String =>
            Try(JacksonJson.asJson(value.asInstanceOf[String])) match {
              case Failure(e) => raiseException("Invalid json.", e)
              case Success(json) =>
                Try(json.sql(fields, !withStructure)) match {
                  case Success(jn) => jn.toString
                  case Failure(e) => raiseException(s"A KCQL exception occurred.${e.getMessage}", e)
                }
            }

          case b: Array[Byte] =>
            Try(JacksonJson.mapper.readTree(b)) match {
              case Failure(e) => raiseException("Invalid json.", e)
              case Success(json) =>
                Try(json.sql(fields, !withStructure)) match {
                  case Failure(e) => raiseException(s"A KCQL exception occurred. ${e.getMessage}", e)
                  case Success(jn) => jn.toString
                }
            }
          //we take it as String
          case other => raiseException(s"Value:$other is not handled!", null)
        }
      }
    }
  }
}


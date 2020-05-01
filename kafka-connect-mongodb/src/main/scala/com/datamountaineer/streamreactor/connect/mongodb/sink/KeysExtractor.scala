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

package com.datamountaineer.streamreactor.connect.mongodb.sink

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data._
import org.json4s.JsonAST._

import scala.annotation.tailrec
import scala.collection.immutable.ListSet


object KeysExtractor {

  private val ISO_DATE_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  private val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  ISO_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"))

  def fromStruct(struct: Struct, keys: Set[String]): ListSet[(String, Any)] = {
    // SCALA 2.12 WARNING: remove the 'reverse' when you upgrade to 2.12;
    // the insertion order of ListSet.toList is preserved in 2.12:
    ListSet( keys.toList.reverse.map { key =>
      val segments = key.split('.').toIndexedSeq

      @tailrec
      def getValue(remainingSegments: Seq[String], struct: Struct): Any = {
        remainingSegments.size match {
          case 0 => throw new Exception("shouldn't get here")
          case 1 => {
            val seg = remainingSegments.head
            val value: AnyRef = struct.get(seg)
            val schema = struct.schema().field(seg).schema()

            val v = schema.`type`() match {
              case Schema.Type.INT32 =>
                if (schema != null && Date.LOGICAL_NAME == schema.name) ISO_DATE_FORMAT.format(Date.toLogical(schema, value.asInstanceOf[Int]))
                else if (schema != null && Time.LOGICAL_NAME == schema.name) TIME_FORMAT.format(Time.toLogical(schema, value.asInstanceOf[Int]))
                else value

              case Schema.Type.INT64 =>
                if (Timestamp.LOGICAL_NAME == schema.name) Timestamp.fromLogical(schema, value.asInstanceOf[(java.util.Date)])
                else value

              case Schema.Type.STRING => value.asInstanceOf[CharSequence].toString

              case Schema.Type.BYTES =>
                if (Decimal.LOGICAL_NAME == schema.name) value.asInstanceOf[BigDecimal].toDouble
                else throw new ConfigException(s"Schema.Type.BYTES is not supported for $key.")

              case Schema.Type.ARRAY =>
                throw new ConfigException(s"Schema.Type.ARRAY is not supported for $key.")

              case Schema.Type.MAP => throw new ConfigException(s"Schema.Type.MAP is not supported for $key.")
              case Schema.Type.STRUCT => throw new ConfigException(s"Schema.Type.STRUCT is not supported for $key.")
              case other => throw new ConfigException(s"$other is not supported for $key.")
            }
            v
          }
          case _ => getValue(remainingSegments.tail, struct.get(remainingSegments.head).asInstanceOf[Struct])
        }
      }

      val shortKey = key.trim.split('.').last
      val value = getValue(segments, struct)
      (shortKey, value)
    }:_*)
  }

  def fromMap(map: java.util.Map[String, Any], keys: Set[String]): ListSet[(String, Any)] = {

    // SCALA 2.12 WARNING: remove the 'reverse' when you upgrade to 2.12;
    // the insertion order of ListSet.toList is preserved in 2.12:
    ListSet( keys.toList.reverse.map{ longKey =>
      val segments = longKey.split('.').toIndexedSeq

      @tailrec
      def getValue(remainingSegments: Seq[String], map: java.util.Map[String, Any]): Any = {
        remainingSegments.size match {
          case 0 => throw new Exception("shouldn't get here")
          case 1 => {
            val seg = remainingSegments.head
            map.get(seg) match {
              case t: String => t
              case t: Boolean => t
              case t: Int => t
              case t: Long => t
              case t: Double => t
              //type restriction for Mongo
              case t: BigInt => t.toLong
              case t: BigDecimal => t.toDouble
              case t: java.util.Date => t
              case other => throw new ConfigException(s"The key $longKey is not supported for type ${Option(other).map(_.getClass.getName).getOrElse("NULL")}")
            }
          }
          case _ => getValue(remainingSegments.tail, map.get(remainingSegments.head).asInstanceOf[java.util.Map[String, Any]])
        }
      }

      val short = longKey.trim.split('.').last
      val value = getValue(segments, map)
      (short, value)
    }:_*)
  }

  def fromJson(jvalue: JValue, keys: Set[String]): List[(String, Any)] = {

    keys.toList.map{ longKey =>
      val segments = longKey.split('.').toIndexedSeq

      @tailrec
      def getValue(remainingSegments: Seq[String], jv: JValue): Any = {
        remainingSegments.size match {
          case 0 => throw new Exception("shouldn't get here")
          case 1 => {
            val seg = remainingSegments.head
            jv \ seg match {
              case JBool(b) => b
              case JDecimal(d) => d.toDouble //need to do this because of mongo
              case JDouble(d) => d
              case JInt(i) => i.toLong //need to do this because of mongo
              case JLong(l) => l
              case JString(s) => s
              case other => throw new ConfigException(s"Field $longKey is not handled as a key (${other.getClass}). it needs to be a int, long, string, double or decimal")
            }
          }
          case _ => getValue(remainingSegments.tail, jv \ remainingSegments.head)
        }
      }

      val short = longKey.trim.split('.').last
      val value = getValue(segments, jvalue)
      (short, value)
    }
  }
}

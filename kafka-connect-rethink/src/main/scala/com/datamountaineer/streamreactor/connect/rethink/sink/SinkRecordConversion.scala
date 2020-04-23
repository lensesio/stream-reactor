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

package com.datamountaineer.streamreactor.connect.rethink.sink

import java.util

import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s.{DefaultFormats, JValue}

import scala.collection.JavaConverters._

/**
  * Created by stepi on 11/10/16.
  */
object SinkRecordConversion {
  /**
    * Builds a rethink MapObject from a connect struct
    *
    * @param record      - The sink record to create the map object
    * @param primaryKeys - The primary keys list
    * @return
    */
  def fromStruct(record: SinkRecord, primaryKeys: Set[String]): java.util.HashMap[String, Any] = {
    val s = record.value().asInstanceOf[Struct]
    val schema = record.valueSchema()
    val fields = schema.fields()
    val mo = new util.HashMap[String, Any]()
    val connectKey = s"${record.topic()}-${record.kafkaPartition().toString}-${record.kafkaOffset().toString}"

    //set id field
    if (primaryKeys.nonEmpty) {
      mo.put(primaryKeys.head, s.get(primaryKeys.head).toString)
    } else {
      mo.put("id", connectKey)
    }

    //add the sinkrecord fields to the MapObject
    fields.asScala.map(f => buildField(f, s, mo))
    mo
  }

  /**
    * Builds a rethink MapObject from a connect struct
    *
    * @param record      - The sink record to create the map object
    * @param primaryKeys - The primary keys list
    * @return
    */
  def fromMap(record: SinkRecord, map: util.Map[String, Any], primaryKeys: Set[String]): java.util.Map[String, Any] = {
    val connectKey = s"${record.topic()}-${record.kafkaPartition().toString}-${record.kafkaOffset().toString}"

    //set id field
    if (primaryKeys.nonEmpty) {
      map.put("id",
        primaryKeys.map { k =>
          Option(map.get(k)).getOrElse(throw new ConfigException(s"$k key part is not present in the payload at topic:${record.topic} partition:${record.kafkaPartition()} offset:${record.kafkaOffset()}")).toString
        }.mkString("."))
    } else {
      map.put("id", connectKey)
    }
    map
  }

  /**
    * Builds a rethink MapObject from a connect struct
    *
    * @param record      - The sink record to create the map object
    * @param primaryKeys - The primary keys list
    * @return
    */
  def fromJson(record: SinkRecord, jvalue: JValue, primaryKeys: Set[String]): java.util.HashMap[String, Any] = {
    val connectKey = s"${record.topic()}-${record.kafkaPartition().toString}-${record.kafkaOffset().toString}"
    implicit val formats = DefaultFormats
    val map = jvalue.extract[Map[String, Any]]

    val mo = new util.HashMap[String, Any]()
    map.foreach { case (key, value) => buildField(key, value, mo) }
    //set id field
    if (primaryKeys.nonEmpty) {
      mo.put("id",
        primaryKeys.map { k =>
          Option(map.get(k)).getOrElse(throw new ConfigException(s"$k key part is not present in the payload at topic:${record.topic} partition:${record.kafkaPartition()} offset:${record.kafkaOffset()}")).toString
        }.mkString("."))
    } else {
      mo.put("id", connectKey)
    }
    mo
  }

  /**
    * Recursively build a MapObject to represent a field
    *
    * @param field  The field schema to add
    * @param struct The struct to extract the value from
    * @param hm     The HashMap
    **/
  private def buildField(field: Field, struct: Struct, hm: java.util.Map[String, Any]): java.util.Map[String, Any] = {
    field.schema().`type`() match {
      case Type.STRUCT =>
        val nested = struct.getStruct(field.name())
        val schema = nested.schema()
        val fields = schema.fields()
        val mo = new util.HashMap[String, Any]()
        fields.asScala.map(f => buildField(f, nested, mo))
        hm.put(field.name, mo)

      case Type.BYTES =>
        if (field.schema().name() == Decimal.LOGICAL_NAME) {
          val decimal = Decimal.toLogical(field.schema(), struct.getBytes(field.name()))
          hm.put(field.name(), decimal)
        } else {
          val str = new String(struct.getBytes(field.name()), "utf-8")
          hm.put(field.name(), str)
        }

      case _ =>
        val value = field.schema().name() match {
          case Time.LOGICAL_NAME => Time.toLogical(field.schema(), struct.getInt32(field.name()))
          case Timestamp.LOGICAL_NAME => Timestamp.toLogical(field.schema(), struct.getInt64(field.name()))
          case Date.LOGICAL_NAME => Date.toLogical(field.schema(), struct.getInt32(field.name()))
          case Decimal.LOGICAL_NAME => Decimal.toLogical(field.schema(), struct.getBytes(field.name()))
          case _ => struct.get(field.name())
        }
        hm.put(field.name(), value)
    }
    hm
  }

  /**
    * Recursively builds an HashMap from a scala map
    *
    * @param key
    * @param value
    * @param hm
    * @return
    */
  private def buildField(key: String, value: Any, hm: java.util.Map[String, Any]): java.util.Map[String, Any] = {
    value match {
      case map: Map[_, _] =>
        val mo = new util.HashMap[String, Any]()
        map.asInstanceOf[Map[String, Any]].foreach { case (k, v) => buildField(k, v, mo) }
        hm.put(key, mo)

      case lst: List[_] =>
        val list = new util.ArrayList[Any]()
        lst.foreach {
          case m: Map[_, _] =>
            val nestedMap = new util.HashMap[String, Any]()
            m.asInstanceOf[Map[String, Any]].foreach { case (k, v) => buildField(k, v, nestedMap) }
            list.add(nestedMap)

          case m => list.add(m)
        }
        hm.put(key, list)
      case _ =>
        hm.put(key, value)
    }
    hm
  }

}

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

package com.datamountaineer.streamreactor.connect.hbase.avro

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import scala.collection.JavaConverters._

/**
  * Builds a map of functions for extracting the values from an avro record and convert them to bytes
  */
object AvroRecordFieldExtractorMapFn {
  /**
    *
    * @param schema - The source avro schema
    * @param fields - The fields present in the schema for which it should return the extractor function
    * @return A map of functions converting the avro field value to bytes depending on the avro field type
    */
  def apply(schema: Schema, fields: Seq[String]): Map[String, (Any) => Array[Byte]] = {
    fields.map { fn =>
      val f = schema.getField(fn)
      if (f == null) {
        throw new IllegalArgumentException(s"$fn does not exist in the given schema.")
      }
      fn -> getFunc(f.schema())
    }.toMap
  }

  private def getFunc(schema: Schema): (Any) => Array[Byte] = {
    val `type` = schema.getType.getName

    `type`.toUpperCase() match {
      case "BOOLEAN" => (v: Any) => if (v == null) null else v.fromBoolean()
      case "BYTES" => (v: Any) => if (v == null) null else v.asInstanceOf[Array[Byte]]
      case "DOUBLE" => (v: Any) => if (v == null) null else v.fromDouble()
      case "FLOAT" => (v: Any) => if (v == null) null else v.fromFloat()
      case "INT" => (v: Any) => if (v == null) null else v.fromInt()
      case "LONG" => (v: Any) => if (v == null) null else v.fromLong()
      case "STRING" => (v: Any) => if (v == null) null else v.fromString()
      case "UNION" =>
        schema.getTypes.asScala.collectFirst {
          case s if s.getType != Type.NULL => getFunc(s)
        }.getOrElse(throw new IllegalArgumentException(s"$schema is not supported."))
      case _ =>
        throw new IllegalArgumentException(s"${schema.getType.name()} is not supported")
    }
  }
}

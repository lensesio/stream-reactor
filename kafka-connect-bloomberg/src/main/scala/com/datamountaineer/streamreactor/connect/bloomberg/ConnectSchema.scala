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

package com.datamountaineer.streamreactor.connect.bloomberg

import org.apache.kafka.connect.data.{Schema, SchemaBuilder}

import scala.collection.JavaConverters._

/**
  * Utility class to allow generating the connect schema for the BloombergData.
  *
  * @param namespace Avro namespace
  */
private[bloomberg] class ConnectSchema(namespace: String) {
  /**
    * Creates an avro schema for the given input. Only a handful of types are supported given the return types from BloombergFieldValueFn
    *
    * @param name field name
    * @param value The value for which it will create a avro schema
    * @return A avro Schema instance
    */
  def createSchema(name: String, value: Any): Schema = {
    value match {
      case _: Boolean => Schema.BOOLEAN_SCHEMA
      case _: Int => Schema.INT32_SCHEMA
      case _: Long => Schema.INT64_SCHEMA
      case _: Double => Schema.FLOAT64_SCHEMA
      case _: Char => Schema.STRING_SCHEMA
      case _: String => Schema.STRING_SCHEMA
      case _: Float => Schema.FLOAT32_SCHEMA
      case list: java.util.List[_] =>
        val firstItemSchema = if (list.isEmpty) Schema.OPTIONAL_STRING_SCHEMA else createSchema(name, list.get(0))
        SchemaBuilder.array(firstItemSchema).build()

      case map: java.util.LinkedHashMap[String @unchecked, _] =>
        val recordBuilder = SchemaBuilder.struct()
        recordBuilder.name(name)
        map.entrySet().asScala.foreach(kvp =>
          recordBuilder.field(kvp.getKey, createSchema(kvp.getKey, kvp.getValue)))
        recordBuilder.build()
      case v => sys.error(s"${v.getClass} is not handled.")
    }
  }
}

object ConnectSchema {
  val namespace = "com.datamountaineer.streamreactor.connect.bloomberg"

  val connectSchema = new ConnectSchema(namespace)

  implicit class BloombergDataToConnectSchema(val data: BloombergData) {
    def getConnectSchema  : Schema = {
      connectSchema.createSchema("BloombergData", data.data)
    }
  }
}

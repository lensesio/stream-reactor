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

package com.datamountaineer.streamreactor.connect.bloomberg.avro

import com.datamountaineer.streamreactor.connect.bloomberg.BloombergData
import com.datamountaineer.streamreactor.connect.bloomberg.avro.AvroSchemaGenerator._
import com.fasterxml.jackson.databind.node.TextNode
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConverters._

/**
  * Utility class to allow generating the avro schema for the data contained by an instance of Bloomberg data.
  *
  * @param namespace Avro schema namespace
  */
private[bloomberg] class AvroSchemaGenerator(namespace: String) {
  private val defaultValue: Object = null

  /**
    * Creates an avro schema for the given input. Only a handful of types are supported given the return types from BloombergFieldValueFn
    *
    * @param name  The field name; if the value is a Map it will create a record with this name
    * @param value The value for which it will create a avro schema
    * @return An avro Schema instance
    */
  def create(name: String, value: Any, allowOptional: Boolean = false): Schema = {
    value match {
      case _: Boolean => getSchemaForType(Schema.Type.BOOLEAN, allowOptional)
      case _: Int => getSchemaForType(Schema.Type.INT, allowOptional)
      case _: Long => getSchemaForType(Schema.Type.LONG, allowOptional)
      case _: Double => getSchemaForType(Schema.Type.DOUBLE, allowOptional)
      case _: Char => getSchemaForType(Schema.Type.STRING, allowOptional)
      case _: String => getSchemaForType(Schema.Type.STRING, allowOptional)
      case _: Float => getSchemaForType(Schema.Type.FLOAT, allowOptional)
      case list: java.util.List[_] =>
        val firstItemSchema = if (list.isEmpty) {
                                  Schema.create(Schema.Type.NULL) }
                              else {
                                  getSchema(create(name, list.get(0)))
                              }
        getSchema(Schema.createArray(firstItemSchema), allowOptional)
      case map: java.util.LinkedHashMap[String @unchecked, _] =>
        val record = Schema.createRecord(name, null, namespace, false)
        val fields = new java.util.ArrayList[Schema.Field](map.size())
        map.entrySet().asScala.foreach { kvp =>
          val field = new Field(kvp.getKey, create(kvp.getKey, kvp.getValue, allowOptional = true), null, defaultValue)
          fields.add(field)
        }
        record.setFields(fields)
        getSchema(record, allowOptional)
      case v => throw new ConnectException(s"${v.getClass} is not handled.")
    }
  }

}

object AvroSchemaGenerator {
  val DefaultNamespace = "com.datamountaineer.streamreactor.connect.bloomberg"

  val Instance = new AvroSchemaGenerator(DefaultNamespace)

  /**
    * Creates a schema allowing null values
    *
    * @param schema Avro schema to create a union with
    * @return
    */
  def optionalSchema(schema: Schema): Schema = {
    Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), schema))
  }

  /**
    * Creates a schema allowing null values
    *
    * @param schemaType Schema type to create union with
    * @return
    */
  def optionalSchema(schemaType: Schema.Type): Schema = {
    val schema = Schema.create(schemaType)
    if (schemaType == Schema.Type.STRING) {
      schema.addProp("avro.java.string", new TextNode("String"))
    }
    Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), schema))
  }

  /**
    * If the optional flag is set will allow null for the given field.
    *
    * @param schema   The source schema
    * @param optional If true it will create a schema allowing nulls
    * @return An instance of Schema
    */
  def getSchema(schema: Schema, optional: Boolean = false) : Schema= {
    if (optional) {
      optionalSchema(schema)
    }
    else {
      schema
    }
  }

  def getSchemaForType(schemaType: Schema.Type, optional: Boolean = false) : Schema = {
    if (optional) {
      optionalSchema(schemaType)
    }
    else {
      val schema = Schema.create(schemaType)
      if (schemaType == Schema.Type.STRING) {
        schema.addProp("avro.java.string", new TextNode("String"))
      }
      schema
    }
  }

  implicit class BloombergDataToAvroSchema(val data: BloombergData)  {
    def getSchema : Schema = Instance.create("BloombergData", data.data)
  }

}

package com.datamountaineer.streamreactor.connect.bloomberg.avro

import com.datamountaineer.streamreactor.connect.bloomberg.BloombergData
import org.apache.avro.Schema
import AvroSchemaGenerator._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.TextNode

import scala.collection.JavaConverters._

/**
  * Utility class to allow generating the avro schema for the data contained by an instance of Bloomberg data.
  *
  * @param namespace
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
        val firstItemSchema = if (list.isEmpty) Schema.create(Schema.Type.NULL) else getSchema(create(name, list.get(0), false), false)
        getSchema(Schema.createArray(firstItemSchema), allowOptional)
      case map: java.util.LinkedHashMap[String, _] =>
        val record = Schema.createRecord(name, null, namespace, false)
        val fields = new java.util.ArrayList[Schema.Field](map.size())
        map.entrySet().asScala.foreach { case kvp =>
          val field = new Schema.Field(kvp.getKey, create(kvp.getKey, kvp.getValue, true), null, defaultValue)
          fields.add(field)
        }
        record.setFields(fields)
        getSchema(record, allowOptional)
      case v => sys.error(s"${v.getClass} is not handled.")
    }
  }

}

object AvroSchemaGenerator {
  val DefaultNamespace = "com.datamountaineer.streamreactor.connect.bloomberg"

  val Instance = new AvroSchemaGenerator(DefaultNamespace)

  /**
    * Creates a schema allowing null values
    *
    * @param schema
    * @return
    */
  def optionalSchema(schema: Schema): Schema = {
    Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), schema))
  }

  /**
    * Creates a schema allowing null values
    *
    * @param schemaType
    * @return
    */
  def optionalSchema(schemaType: Schema.Type): Schema = {
    val schema = Schema.create(schemaType)
    if (schemaType == Schema.Type.STRING)
      schema.addProp("avro.java.string", new TextNode("String"))
    Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), schema))
  }

  /**
    * If the optional flag is set will allow null for the given field.
    *
    * @param schema   The source schema
    * @param optional If true it will create a schema allowing nulls
    * @return An instance of Schema
    */
  def getSchema(schema: Schema, optional: Boolean = false) = {
    if (optional) optionalSchema(schema)
    else schema
  }

  def getSchemaForType(schemaType: Schema.Type, optional: Boolean = false) = {
    if (optional) optionalSchema(schemaType)
    else {
      val schema = Schema.create(schemaType)
      if (schemaType == Schema.Type.STRING) {
        schema.addProp("avro.java.string", new TextNode("String"))
      }
      schema
    }
  }

  implicit class BloombergDataToAvroSchema(val data: BloombergData) {
    def getSchema = Instance.create("BloombergData", data.data)
  }

}
package com.datamountaineer.streamreactor.connect.bloomberg

import org.apache.avro.Schema

import scala.collection.JavaConverters._

/**
  * Utility class to allow generating the avro schema for the BloombergData.
  *
  * @param namespace
  */
private[bloomberg] class AvroSchema(namespace: String) {
  private val defaultValue: Object = null

  /**
    * Creates an avro schema for the given input. Only a handful of types are supported given the return types from BloombergFieldValueFn
    *
    * @param name
    * @param value The value for which it will create a avro schema
    * @return A avro Schema instance
    */
  def createSchema(name: String, value: Any): Schema = {
    value match {
      case _: Boolean => Schema.create(Schema.Type.BOOLEAN)
      case _: Int => Schema.create(Schema.Type.INT)
      case _: Long => Schema.create(Schema.Type.LONG)
      case _: Double => Schema.create(Schema.Type.DOUBLE)
      case _: Char => Schema.create(Schema.Type.STRING)
      case _: String => Schema.create(Schema.Type.STRING)
      case _: Float => Schema.create(Schema.Type.FLOAT)
      case list: java.util.List[Any] =>
        val firstItemSchema = if (list.isEmpty) Schema.create(Schema.Type.NULL) else createSchema(name, list.get(0))
        Schema.createArray(firstItemSchema)
      case map: java.util.LinkedHashMap[String, Any] =>
        val record = Schema.createRecord(name, null, namespace, false)
        val fields = new java.util.ArrayList[Schema.Field](map.size())
        map.entrySet().asScala.foreach { case kvp =>
          val field = new Schema.Field(kvp.getKey, createSchema(kvp.getKey, kvp.getValue), null, defaultValue)
          fields.add(field)
        }
        record.setFields(fields)
        record
      case v => sys.error(s"${v.getClass} is not handled.")
    }
  }
}

object AvroSchema {
  val namespace = "com.datamountaineer.streamreactor.connect.bloomberg"

  val avroSchema = new AvroSchema(namespace)

  implicit class BloombergDataToAvroSchema(val data: BloombergData) {
    def getSchema = {
      avroSchema.createSchema("BloombergData", data.asMap)
    }
  }

}
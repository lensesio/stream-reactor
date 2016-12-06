package com.datamountaineer.streamreactor.connect.mqtt.source.converters

import java.nio.charset.Charset
import java.util

import com.datamountaineer.streamreactor.connect.mqtt.source.MqttMsgKey
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord


class JsonSimpleConverter extends MqttConverter {

  override def initialize(map: Map[String, String]): Unit = {
  }

  override def convert(kafkaTopic: String, mqttSource: String, messageId: Int, bytes: Array[Byte]): SourceRecord = {
    require(bytes != null, s"Invalid $bytes parameter")
    val json = new String(bytes, Charset.defaultCharset)
    val schemaAndValue = JsonSimpleConverter.convert(mqttSource, json)
    new SourceRecord(null,
      null,
      kafkaTopic,
      MqttMsgKey.schema,
      MqttMsgKey.getStruct(mqttSource, messageId),
      schemaAndValue.schema(),
      schemaAndValue.value())
  }
}

object JsonSimpleConverter {

  import org.json4s._
  import org.json4s.native.JsonMethods._

  def convert(name: String, str: String): SchemaAndValue = convert(name, parse(str))

  def convert(name: String, value: JValue): SchemaAndValue = {
    value match {
      case JArray(arr) =>
        val values = new util.ArrayList[AnyRef]()
        val sv = convert(name, arr.head)
        values.add(sv.value())
        arr.tail.foreach { v => values.add(convert(name, v).value()) }

        val schema = SchemaBuilder.array(sv.schema()).optional().build()
        new SchemaAndValue(schema, values)
      case JBool(b) => new SchemaAndValue(Schema.BOOLEAN_SCHEMA, b)
      case JDecimal(d) =>
        val schema = Decimal.builder(d.scale).optional().build()
        new SchemaAndValue(schema, Decimal.fromLogical(schema, d.bigDecimal))
      case JDouble(d) => new SchemaAndValue(Schema.FLOAT64_SCHEMA, d)
      case JInt(i) => new SchemaAndValue(Schema.INT64_SCHEMA, i.toLong) //on purpose! LONG (we might get later records with long entries)
      case JLong(l) => new SchemaAndValue(Schema.INT64_SCHEMA, l)
      case JNull => new SchemaAndValue(Schema.STRING_SCHEMA, null)
      case JString(s) => new SchemaAndValue(Schema.STRING_SCHEMA, s)
      case JObject(values) =>
        val builder = SchemaBuilder.struct().name(name)

        val fields = values.map { case (n, v) =>
          val schemaAndValue = convert(n, v)
          builder.field(n, schemaAndValue.schema())
          n -> schemaAndValue.value()
        }.toMap
        val schema = builder.build()

        val struct = new Struct(schema)
        fields.foreach { case (field, v) => struct.put(field, v) }

        new SchemaAndValue(schema, struct)
    }
  }
}

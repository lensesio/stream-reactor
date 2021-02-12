/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.converters.source

import java.nio.charset.Charset
import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.common.converters.MsgKey
import io.confluent.connect.avro.AvroData
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord

/**
  * Experimental
  */
class JsonConverterWithSchemaEvolution extends Converter {
  private val avroData = new AvroData(4)
  implicit private var latestSchema: Option[Schema] = None


  override def convert(kafkaTopic: String,
                       mqttSource: String,
                       messageId: String,
                       bytes: Array[Byte],
                       keys: Seq[String] = Seq.empty,
                       keyDelimiter: String = ".",
                       properties: Map[String, String] = Map.empty): SourceRecord = {
    require(bytes != null, s"Invalid $bytes parameter")
    val json = new String(bytes, Charset.defaultCharset)
    val schemaAndValue = JsonConverterWithSchemaEvolution.convert(mqttSource, json)
    latestSchema = Some(schemaAndValue.schema())

    val value = schemaAndValue.value()
    value match {
      case s: Struct if keys.nonEmpty =>
        val keysValue = keys.flatMap { key =>
          Option(KeyExtractor.extract(s, key.split('.').toVector)).map(_.toString)
        }.mkString(keyDelimiter)

        new SourceRecord(null,
          Collections.singletonMap(JsonConverterWithSchemaEvolution.ConfigKey, latestSchema.map(avroData.fromConnectSchema(_).toString).orNull),
          kafkaTopic,
          Schema.STRING_SCHEMA,
          keysValue,
          schemaAndValue.schema(),
          schemaAndValue.value())

      case _ =>
        new SourceRecord(null,
          Collections.singletonMap(JsonConverterWithSchemaEvolution.ConfigKey, latestSchema.map(avroData.fromConnectSchema(_).toString).orNull),
          kafkaTopic,
          MsgKey.schema,
          MsgKey.getStruct(mqttSource, messageId),
          schemaAndValue.schema(),
          schemaAndValue.value())
    }

  }
}

object JsonConverterWithSchemaEvolution {

  val ConfigKey = "JsonConverterWithSchemaEvolution.Schema"

  import org.json4s._
  import org.json4s.native.JsonMethods._

  def convert(name: String, str: String)(implicit schema: Option[Schema]): SchemaAndValue = convert(name, parse(str))

  def convert(name: String, value: JValue)(implicit aggregatedSchema: Option[Schema]): SchemaAndValue = {
    value match {
      case JArray(arr) =>
        val values = new util.ArrayList[AnyRef]()
        val prevSchema = aggregatedSchema.map(_.field(name)).map(_.schema)
        val sv = convert(name, arr.head)(prevSchema)
        values.add(sv.value())
        arr.tail.foreach { v => values.add(convert(name, v)(prevSchema).value()) }

        val schema = SchemaBuilder.array(sv.schema()).optional().build()
        new SchemaAndValue(schema, values)
      case JBool(b) => new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, b)
      case JDecimal(d) =>
        val schema = Decimal.builder(d.scale).optional().build()
        new SchemaAndValue(schema, Decimal.fromLogical(schema, d.bigDecimal))
      case JDouble(d) => new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, d)
      case JInt(i) => new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, i.toLong) //on purpose! LONG (we might get later records with long entries)
      case JLong(l) => new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, l)
      case JNull | JNothing => new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null)
      case JString(s) => new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, s)
      case JObject(values) =>
        val builder = SchemaBuilder.struct().name(name)

        val fields = values.map { case (n, v) =>
          val prevSchema = aggregatedSchema.map(_.field(n)).map(_.schema())
          val schemaAndValue = convert(n, v)(prevSchema)
          builder.field(n, schemaAndValue.schema())
          n -> schemaAndValue.value()
        }.toMap
        val schema = builder.build()

        import scala.collection.JavaConverters._
        aggregatedSchema
          .foreach { schema =>
            schema.fields().asScala
              .withFilter(f => !fields.contains(f.name()))
              .foreach { f =>
                builder.field(f.name(), f.schema())
              }
          }

        val struct = new Struct(schema)
        fields.foreach { case (field, v) => struct.put(field, v) }

        new SchemaAndValue(schema, struct)
    }
  }
}

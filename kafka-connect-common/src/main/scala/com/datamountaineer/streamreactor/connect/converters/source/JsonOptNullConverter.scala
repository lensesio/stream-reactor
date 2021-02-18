/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.connect.converters.source

import com.datamountaineer.streamreactor.common.converters.MsgKey

import java.nio.charset.Charset
import java.util
import java.util.Collections
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord


class JsonOptNullConverter extends Converter {
  override def convert(kafkaTopic: String,
                       sourceTopic: String,
                       messageId: String,
                       bytes: Array[Byte],
                       keys:Seq[String] = Seq.empty,
                       keyDelimiter:String = ".",
                       properties: Map[String, String] = Map.empty): SourceRecord = {
    if(bytes == null) throw new ConnectException("Invalid input. Input cannot be null.")
    val json = new String(bytes, Charset.defaultCharset)
    val schemaAndValue = JsonOptNullConverter.convert(sourceTopic, json)
    val value = schemaAndValue.value()
    value match {
      case s:Struct if keys.nonEmpty =>
        val keysValue = keys.flatMap { key =>
          Option(KeyExtractor.extract(s, key.split('.').toVector)).map(_.toString)
        }.mkString(keyDelimiter)

        new SourceRecord(Collections.singletonMap(Converter.TopicKey, sourceTopic),
          null,
          kafkaTopic,
          Schema.STRING_SCHEMA,
          keysValue,
          schemaAndValue.schema(),
          schemaAndValue.value())
      case _=>
        new SourceRecord(Collections.singletonMap(Converter.TopicKey, sourceTopic),
          null,
          kafkaTopic,
          MsgKey.schema,
          MsgKey.getStruct(sourceTopic, messageId),
          schemaAndValue.schema(),
          schemaAndValue.value())
    }

  }
}

object JsonOptNullConverter {

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
      case JNull | JNothing => new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, null)
      case JString(s) => new SchemaAndValue(Schema.STRING_SCHEMA, s)
      case JObject(values) =>
        val builder = SchemaBuilder.struct().name(name.replace("/", "_"))

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

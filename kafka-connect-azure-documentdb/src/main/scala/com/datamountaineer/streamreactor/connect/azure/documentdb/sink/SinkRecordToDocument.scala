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
package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import com.datamountaineer.streamreactor.connect.azure.documentdb.config.DocumentDbSinkSettings
import com.datamountaineer.streamreactor.connect.azure.documentdb.converters.SinkRecordConverter
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.microsoft.azure.documentdb.Document
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.sink.SinkRecord

object SinkRecordToDocument extends ConverterUtil {
  def apply(record: SinkRecord, keys: Set[String] = Set.empty)(implicit settings: DocumentDbSinkSettings): (Document, Iterable[(String, Any)]) = {
    val schema = record.valueSchema()
    val value = record.value()

    if (schema == null) {
      //try to take it as string
      value match {
        case _: java.util.Map[_, _] =>

          val fields = settings.fields(record.topic())
          val extracted = convertSchemalessJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          //not ideal; but the compile is hashmap anyway

          SinkRecordConverter.fromMap(extracted.asInstanceOf[java.util.Map[String, AnyRef]]) ->
            keys.headOption.map(_ => KeysExtractor.fromMap(extracted, keys)).getOrElse(Iterable.empty)

        case _: String =>
          val extracted = convertStringSchemaAndJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromJson(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromJson(extracted, keys)).getOrElse(Iterable.empty)

        case _ => sys.error("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          val extracted = convertStringSchemaAndJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromJson(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromJson(extracted, keys)).getOrElse(Iterable.empty)

        case Schema.Type.STRUCT =>
          val extracted = convert(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromStruct(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromStruct(extracted.value().asInstanceOf[Struct], keys)).getOrElse(Iterable.empty)

        case other => sys.error(s"$other schema is not supported")
      }
    }
  }
}

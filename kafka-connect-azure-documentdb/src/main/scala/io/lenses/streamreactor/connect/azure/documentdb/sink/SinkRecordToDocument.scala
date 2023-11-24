/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.documentdb.sink

import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.azure.documentdb.config.DocumentDbSinkSettings
import io.lenses.streamreactor.connect.azure.documentdb.converters.SinkRecordConverter
import com.microsoft.azure.documentdb.Document
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.annotation.nowarn

@nowarn
object SinkRecordToDocument extends ConverterUtil {
  def apply(
    record: SinkRecord,
    keys:   Set[String] = Set.empty,
  )(
    implicit
    settings: DocumentDbSinkSettings,
  ): (Document, Iterable[(String, Any)]) = {
    val schema = record.valueSchema()
    val value  = record.value()

    if (schema == null) {
      //try to take it as string
      value match {
        case _: java.util.Map[_, _] =>
          @nowarn
          val extracted =
            convertSchemalessJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromMap(extracted.asInstanceOf[java.util.Map[String, AnyRef]]) ->
            keys.headOption.map(_ => KeysExtractor.fromMap(extracted, keys)).getOrElse(Iterable.empty)

        case _: String =>
          @nowarn
          val extracted =
            convertFromStringAsJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          extracted match {
            case Right(r) =>
              SinkRecordConverter.fromJson(r.converted) ->
                keys.headOption.map(_ => KeysExtractor.fromJson(r.converted, keys)).getOrElse(Iterable.empty)
            case Left(l) => throw new ConnectException(l)
          }

        case _ => throw new ConnectException("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          @nowarn
          val extracted =
            convertFromStringAsJson(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          extracted match {
            case Right(r) =>
              SinkRecordConverter.fromJson(r.converted) ->
                keys.headOption.map(_ => KeysExtractor.fromJson(r.converted, keys)).getOrElse(Iterable.empty)
            case Left(l) => throw new ConnectException(l)
          }

        case Schema.Type.STRUCT =>
          @nowarn
          val extracted = convert(record, settings.fields(record.topic()), settings.ignoredField(record.topic()))
          SinkRecordConverter.fromStruct(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromStruct(extracted.value().asInstanceOf[Struct], keys)).getOrElse(
              Iterable.empty,
            )

        case other => throw new ConnectException(s"[$other] schema is not supported")
      }
    }
  }
}

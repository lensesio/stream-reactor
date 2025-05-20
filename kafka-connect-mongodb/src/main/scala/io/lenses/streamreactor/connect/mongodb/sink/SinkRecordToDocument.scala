/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.mongodb.sink

import io.lenses.streamreactor.connect.mongodb.config.MongoSettings
import io.lenses.streamreactor.connect.mongodb.converters.SinkRecordConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document

import scala.annotation.nowarn

object SinkRecordToDocument extends ConverterUtilProxy {
  def apply(
    record: SinkRecord,
    keys:   Set[String] = Set.empty,
  )(
    implicit
    settings: MongoSettings,
  ): (Document, Iterable[(String, Any)]) = {
    val schema = record.valueSchema()
    val value  = record.value()
    val fields = settings.fields.getOrElse(record.topic(), Map.empty)

    val allFields = if (fields.size == 1 && fields.head._1 == "*") true else false

    if (schema == null) {
      //try to take it as string
      value match {
        case _: java.util.Map[_, _] =>
          val extracted = convertSchemalessJson(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty),
          )
          //not ideal; but the compile is hashmap anyway

          SinkRecordConverter.fromMap(extracted.asInstanceOf[java.util.Map[String, AnyRef]]) ->
            keys.headOption.map(_ => KeysExtractor.fromMap(extracted, keys)).getOrElse(Iterable.empty)
        case _ => throw new ConnectException("For schemaless record only String and Map types are supported")
      }
    } else {
      schema.`type`() match {
        case Schema.Type.STRING =>
          convertFromStringAsJson(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty),
            includeAllFields = allFields,
          ) match {
            case Right(ConversionResult(original, extracted)) =>
              val extractedKeys =
                keys.headOption.map(_ => KeysExtractor.fromJson(original, keys)).getOrElse(Iterable.empty)
              SinkRecordConverter.fromJson(extracted) -> extractedKeys

            case Left(value) =>
              //This needs full refactor to cleanup and write FP style scala
              throw new ConnectException(value)
          }
        case Schema.Type.STRUCT =>
          @nowarn val extracted = convert(
            record,
            fields,
            settings.ignoredField.getOrElse(record.topic(), Set.empty),
          )
          SinkRecordConverter.fromStruct(extracted) ->
            keys.headOption.map(_ => KeysExtractor.fromStruct(record.value().asInstanceOf[Struct], keys)).getOrElse(
              Iterable.empty,
            )

        case other => throw new ConnectException(s"$other schema is not supported")
      }
    }
  }
}

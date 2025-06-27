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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter

import com.azure.cosmos.implementation.Document
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeySource
import io.lenses.streamreactor.connect.azure.cosmosdb.converters.SinkRecordConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.annotation.nowarn

@nowarn
object SinkRecordToDocument extends ConverterUtil {
  def apply(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
    idGenerator:   KeySource,
  ): Document = {

    val document = Option(record.valueSchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) => convertStringToDocument(record, fields, ignoredFields)
      case Some(Schema.Type.STRUCT) => convertStructToDocument(record, fields, ignoredFields)
      case Some(other)              => throw new ConnectException(s"[$other] schema is not supported")
      case None =>
        record.value() match {
          case _: util.Map[_, _] => convertMapToDocument(record, fields, ignoredFields)
          case _: String         => convertStringToDocument(record, fields, ignoredFields)
          case _ => throw new ConnectException("For schemaless record only String and Map types are supported")
        }
    }
    document.setId(generateDocumentId(record, idGenerator))

  }

  private def convertStringToDocument(record: SinkRecord, fields: Map[String, String], ignoredFields: Set[String]) = {
    val extracted =
      convertFromStringAsJson(record, fields, ignoredFields)
    extracted match {
      case Right(r) =>
        SinkRecordConverter.fromJson(r.converted)
      case Left(l) => throw new ConnectException(l)
    }
  }

  private def convertMapToDocument(record: SinkRecord, fields: Map[String, String], ignoredFields: Set[String]) = {
    val extracted =
      convertSchemalessJson(record, fields, ignoredFields)
    SinkRecordConverter.fromMap(extracted.asInstanceOf[util.Map[String, AnyRef]])
  }

  private def convertStructToDocument(record: SinkRecord, fields: Map[String, String], ignoredFields: Set[String]) = {
    val extracted = convert(record, fields, ignoredFields)
    SinkRecordConverter.fromStruct(extracted)
  }

  private def generateDocumentId(record: SinkRecord, idGenerator: KeySource) =
    idGenerator.generateId(record) match {
      case Left(value) => throw new ConnectException("error generating id field", value)
      case Right(null) =>
        throw new ConnectException("id field value is null, please check your configuration")
      case Right(value: String) => value
      case Right(value: AnyRef) =>
        throw new ConnectException(s"id field value not a String, it is a ${value.getClass.getName}")
    }
}

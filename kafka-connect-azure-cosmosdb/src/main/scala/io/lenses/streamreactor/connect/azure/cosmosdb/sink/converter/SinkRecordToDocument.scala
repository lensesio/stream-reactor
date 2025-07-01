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

import cats.implicits.toBifunctorOps
import com.azure.cosmos.implementation.Document
import io.lenses.streamreactor.common.schemas.ConverterUtil
import io.lenses.streamreactor.connect.azure.cosmosdb.config.KeySource
import io.lenses.streamreactor.connect.azure.cosmosdb.converters.SinkRecordConverterEither
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import java.util
import scala.annotation.nowarn
import scala.util.Try

/**
 * Utility for converting Kafka SinkRecords into Azure Cosmos DB Document objects.
 * Handles different value types (Struct, String, Map) and applies field projections, ignored fields, and ID generation.
 * All conversion methods return Either[Throwable, Document] to robustly handle errors from underlying libraries.
 */
@nowarn
object SinkRecordToDocument extends ConverterUtil {

  /**
   * Converts a SinkRecord into a Cosmos DB Document, applying field projections, ignored fields, and generating the document ID.
   *
   * @param record        The Kafka SinkRecord to convert.
   * @param fields        Map of field projections (Kafka field -> Document field).
   * @param ignoredFields Set of field names to ignore in the output document.
   * @param idGenerator   Strategy for generating the document ID.
   * @return              Either a Throwable (on error) or the resulting Document.
   */
  def apply(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
    idGenerator:   KeySource,
  ): Either[Throwable, Document] =
    for {
      document <- getDocument(record, fields, ignoredFields)
      withId   <- generateDocumentId(record, idGenerator).map(document.setId)
    } yield withId

  /**
   * Determines the appropriate conversion method based on the SinkRecord's schema or value type.
   *
   * @param record        The Kafka SinkRecord.
   * @param fields        Field projections.
   * @param ignoredFields Fields to ignore.
   * @return              Either a Throwable or the converted Document.
   */
  private def getDocument(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
  ): Either[Throwable, Document] =
    Option(record.valueSchema()).map(_.`type`()) match {
      case Some(Schema.Type.STRING) => convertStringToDocument(record, fields, ignoredFields)
      case Some(Schema.Type.STRUCT) => convertStructToDocument(record, fields, ignoredFields)
      case Some(other)              => Left(new ConnectException(s"[$other] schema is not supported"))
      case None =>
        record.value() match {
          case _: util.Map[_, _] => convertMapToDocument(record, fields, ignoredFields)
          case _: String         => convertStringToDocument(record, fields, ignoredFields)
          case _ => Left(new ConnectException("For schemaless record only String and Map types are supported"))
        }
    }

  /**
   * Converts a SinkRecord with a String value (typically JSON) to a Document, applying projections and ignored fields.
   *
   * @param record        The SinkRecord.
   * @param fields        Field projections.
   * @param ignoredFields Fields to ignore.
   * @return              Either a Throwable or the converted Document.
   */
  private def convertStringToDocument(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
  ): Either[Throwable, Document] =
    for {
      conversionResult <- convertFromStringAsJson(record, fields, ignoredFields)
        .leftMap(s => new ConnectException(s))
      document <- SinkRecordConverterEither.fromJson(conversionResult.converted)
    } yield document

  /**
   * Converts a schemaless SinkRecord with a Map value to a Document, applying projections and ignored fields.
   *
   * @param record        The SinkRecord.
   * @param fields        Field projections.
   * @param ignoredFields Fields to ignore.
   * @return              Either a Throwable or the converted Document.
   */
  private def convertMapToDocument(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
  ): Either[Throwable, Document] =
    for {
      map      <- Try(convertSchemalessJson(record, fields, ignoredFields).asInstanceOf[util.Map[String, AnyRef]]).toEither
      document <- SinkRecordConverterEither.fromMap(map)
    } yield document

  /**
   * Converts a SinkRecord with a Struct value to a Document, applying projections and ignored fields.
   *
   * @param record        The SinkRecord.
   * @param fields        Field projections.
   * @param ignoredFields Fields to ignore.
   * @return              Either a Throwable or the converted Document.
   */
  private def convertStructToDocument(
    record:        SinkRecord,
    fields:        Map[String, String],
    ignoredFields: Set[String],
  ): Either[Throwable, Document] =
    SinkRecordConverterEither.fromStruct(convert(record, fields, ignoredFields))

  /**
   * Generates the document ID using the provided KeySource strategy.
   *
   * @param record      The SinkRecord.
   * @param idGenerator The KeySource strategy.
   * @return            Either a Throwable or the generated ID string.
   */
  private def generateDocumentId(record: SinkRecord, idGenerator: KeySource): Either[Throwable, String] =
    idGenerator.generateId(record) match {
      case Left(t) => Left(t)
      case Right(anyref) =>
        anyref match {
          case null =>
            Left(new ConnectException("id field value is null, please check your configuration"))
          case value: String => Right(value)
          case value: AnyRef =>
            Left(new ConnectException(s"id field value not a String, it is a ${value.getClass.getName}"))
        }
    }
}

/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.common.schemas

import io.lenses.streamreactor.common.config.base.settings.Projections
import io.lenses.streamreactor.common.schemas.StructHelper.StructExtension
import io.lenses.streamreactor.connect.converters.source.JsonSimpleConverter
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SinkRecordConverterHelper extends StrictLogging {

  lazy val simpleJsonConverter = new SimpleJsonConverter()

  implicit final class SinkRecordExtension(val record: SinkRecord) extends AnyVal {

    /**
      * make new sink record, taking fields
      * from the key, value and headers
      */
    def newFilteredRecordAsStruct(projections: Projections): Struct = {

      val ignoredFields = projections.ignoreFields.getOrElse(record.topic(), Set.empty)

      //if we have keys fields and a key value extract
      val keyStruct =
        if (
          projections.keyFields.nonEmpty && projections
            .keyFields(record.topic())
            .nonEmpty && record.key() != null
        ) {
          extract(
            payload       = record.key(),
            payloadSchema = record.keySchema(),
            fields        = projections.keyFields(record.topic()),
            ignoreFields  = ignoredFields,
          )
        } else {
          logger.debug(
            s"Key is null for topic [${record.topic()}], partition [${record
              .kafkaPartition()}], offset [${record.kafkaOffset()}])",
          )
          new Struct(SchemaBuilder.struct().build())
        }

      //if we have value fields and a value extract
      val valueStruct =
        if (
          projections.valueFields.nonEmpty && projections
            .valueFields(record.topic())
            .nonEmpty && record.value() != null
        ) {
          extract(
            payload       = record.value(),
            payloadSchema = record.valueSchema(),
            fields        = projections.valueFields(record.topic()),
            ignoreFields  = ignoredFields,
          )
        } else {
          logger.debug(
            s"Value is null for topic [${record.topic()}], partition [${record
              .kafkaPartition()}], offset [${record.kafkaOffset()}])",
          )
          new Struct(SchemaBuilder.struct().build())
        }

      //if we have headers fields and values extract
      val headerStruct =
        if (
          projections.headerFields.nonEmpty && projections
            .headerFields(record.topic())
            .nonEmpty && !record.headers().isEmpty
        ) {
          val headerAsSinkRecord = headerToSinkRecord(record)
          extract(
            payload       = headerAsSinkRecord.value(),
            payloadSchema = headerAsSinkRecord.valueSchema(),
            fields        = projections.headerFields(record.topic()),
            ignoreFields  = ignoredFields,
          )
        } else {
          logger.debug(
            s"Headers are empty for topic [${record.topic()}], partition [${record
              .kafkaPartition()}], offset [${record.kafkaOffset()}])",
          )
          new Struct(SchemaBuilder.struct().build())
        }

      //create a new struct with the keys, values and headers
      keyStruct ++ valueStruct ++ headerStruct
    }

    //convert headers to sink record
    def headerToSinkRecord(record: SinkRecord): SinkRecord = {
      val schemaBuilder = SchemaBuilder.struct()
      val asScala       = record.headers().asScala
      asScala
        .foreach(h => schemaBuilder.field(h.key(), h.schema()))

      val schema    = schemaBuilder.build()
      val newStruct = new Struct(schema)

      asScala
        .foreach(h => newStruct.put(h.key(), h.value()))

      new SinkRecord("header", 0, null, null, newStruct.schema(), newStruct, 0)
    }

    // create a new struct with the required fields
    def extract(
      payload:       Object,
      payloadSchema: Schema,
      fields:        Map[String, String],
      ignoreFields:  Set[String],
    ): Struct =
      if (payloadSchema == null) {

        val struct = toStructFromJson(payload)
        // converted so now reduce the schema
        struct.reduceSchema(schema = struct.schema(), fields = fields, ignoreFields = ignoreFields)

      } else {
        payloadSchema.`type`() match {
          //struct
          case Schema.Type.STRUCT =>
            payload
              .asInstanceOf[Struct]
              .reduceSchema(schema = payloadSchema, fields = fields, ignoreFields = ignoreFields)

          // json with string schema
          case Schema.Type.STRING =>
            val struct = toStructFromStringAndJson(payload, payloadSchema)
            struct.reduceSchema(schema = struct.schema(), fields = fields, ignoreFields)

          case other =>
            throw new ConnectException(
              s"[$other] schema is not supported for extracting fields for topic [${record
                .topic()}], partition [${record
                .kafkaPartition()}], offset [${record.kafkaOffset()}]",
            )
        }
      }

    //handle json no schema
    private def toStructFromJson(payload: Object): Struct =
      Try(payload.asInstanceOf[java.util.HashMap[String, Any]]) match {
        case Success(_) =>
          convert(new ObjectMapper().writeValueAsString(payload))

        case Failure(_) =>
          throw new ConnectException(
            s"[${payload.getClass}] is not valid. Expecting a Map[String, Any] for topic [${record
              .topic()}], partition [${record.kafkaPartition()}], offset [${record.kafkaOffset()}]",
          )
      }

    //handle json with string schema
    private def toStructFromStringAndJson(payload: AnyRef, payloadSchema: Schema): Struct = {

      val expectedInput = payloadSchema != null && payloadSchema
        .`type`() == Schema.STRING_SCHEMA.`type`()
      if (!expectedInput) {
        throw new ConnectException(
          s"[$payload] is not handled. Expecting Schema.String",
        )
      } else {
        payload match {
          case s: String => convert(s)
          case other =>
            throw new ConnectException(
              s"[${other.getClass}] is not valid. Expecting a Struct",
            )
        }
      }
    }

    private def convert(json: String): Struct = {
      val schemaAndValue = JsonSimpleConverter.convert("", json)
      schemaAndValue.schema().`type`() match {
        case Schema.Type.STRUCT =>
          schemaAndValue.value().asInstanceOf[Struct]

        case other =>
          throw new ConnectException(
            s"[$other] schema is not supported for extracting fields for topic [${record
              .topic()}], partition [${record
              .kafkaPartition()}], offset [${record.kafkaOffset()}]",
          )
      }
    }
  }
}

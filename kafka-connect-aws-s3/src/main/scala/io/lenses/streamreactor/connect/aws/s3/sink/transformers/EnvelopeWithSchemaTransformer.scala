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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.sink.transformers.EnvelopeWithSchemaTransformer.envelope
import org.apache.kafka.connect.data._

import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
  * Creates an envelope for the message detail. It is expected the Key and/or Value, if used to have a Connect schema attached.
  * @param settings The settings for the data storage for the topic
  */
case class EnvelopeWithSchemaTransformer(topic: Topic, settings: DataStorageSettings) extends Transformer {
  def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] =
    if (message.topic != topic) {
      Left(
        new RuntimeException(
          s"Invalid state reached. Envelope transformer topic [${topic.value}] does not match incoming message topic [${message.topic.value}].",
        ),
      )
    } else if (settings.hasEnvelope) {
      envelope(message, settings).asRight
    } else {
      message.asRight
    }

}

object EnvelopeWithSchemaTransformer {
  private val MetadataSchema: Schema = SchemaBuilder.struct()
    .field("timestamp", Schema.INT64_SCHEMA).optional()
    .field("topic", Schema.STRING_SCHEMA)
    .field("partition", Schema.INT32_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  /**
    * Creates an envelope schema for the message detail. This is a schema that contains the key, value and headers and metadata.
    * Key and Value schema is set optional to handle null data (i.e. deletes as tombstones)
    * {{{
    *   {
    *     "key": ...,
    *     "value": ...,
    *     "headers": {
    *       "header1": "value1",
    *       "header2": "value2"
    *     },
    *     "metadata": {
    *       "timestamp": 123456789,
    *       "topic": "topic1",
    *       "partition": 0,
    *       "offset": 1
    *
    *     }
    *   }
    * }}}
    *
    * @return
    */
  private def envelope(message: MessageDetail, settings: DataStorageSettings): MessageDetail = {
    val schema = envelopeSchema(message, settings)

    val envelope = new Struct(schema)
    if (settings.key) {
      //only set the value when is not null. If the schema is null the schema does not contain the field
      putWithOptional(envelope, "key", message.key)
    }
    if (settings.value) {
      putWithOptional(envelope, "value", message.value)
    }
    if (settings.metadata) envelope.put("metadata", metadataData(message))
    if (settings.headers) {
      Option(schema.field("headers")).foreach { field =>
        envelope.put("headers", headersData(message, field.schema()))
      }
    }
    message.copy(value = StructSinkData(envelope))
  }

  /**
    * Converts a SinkData to an optional value. This is used to handle tombstones and null key entries.
    * Since the envelope Key and Value are optional, Connect framework does not allow setting a Struct field when the schema is optional
    * @param value The value to convert
    * @return The value as an optional
    */
  def toOptionalConnectData(value: SinkData): Any =
    value match {
      case StructSinkData(value) if !value.schema().isOptional =>
        val newStruct = new Struct(toOptional(value.schema()))
        value.schema().fields().asScala.foreach { field =>
          newStruct.put(field.name(), value.get(field))
        }
        newStruct

      case _ => value.value
    }

  private def putWithOptional(envelope: Struct, key: String, value: SinkData): Struct =
    Option(envelope.schema().field(key)).fold(envelope) { _ =>
      envelope.put(key, toOptionalConnectData(value))
    }

  private def metadataData(message: MessageDetail): Struct = {
    val metadata = new Struct(MetadataSchema)
    message.timestamp.map(_.toEpochMilli).foreach(metadata.put("timestamp", _))
    metadata.put("topic", message.topic.value)
    metadata.put("partition", message.partition)
    metadata.put("offset", message.offset.value)
    metadata
  }
  def envelopeSchema(message: MessageDetail, settings: DataStorageSettings): Schema = {
    var builder: SchemaBuilder = SchemaBuilder.struct()

    //both key and value are optional to handle tombstones and null key entries

    builder =
      if (settings.key) message.key.schema().fold(builder) { schema =>
        builder.field("key", toOptional(schema))
      }
      else builder
    //set the value schema optional to handle delete records
    builder =
      if (settings.value) message.value.schema().fold(builder) { schema =>
        builder.field("value", toOptional(schema))
      }
      else builder
    builder =
      if (settings.headers) headersSchema(message.headers).fold(builder)(builder.field("headers", _))
      else builder
    builder = if (settings.metadata) builder.field("metadata", MetadataSchema) else builder
    builder.build()
  }

  private def headersSchema(headers: Map[String, SinkData]): Option[Schema] = {
    val headersList = headers.filter(_._2.schema().isDefined).toList.sortBy(_._1)
    headersList.headOption.map { _ =>
      val builder = headersList.foldLeft(SchemaBuilder.struct()) {
        case (builder, (key, data)) =>
          builder.field(key, toOptional(data.schema().get))
      }
      builder.build()
    }
  }

  private def headersData(message: MessageDetail, schema: Schema): Struct = {
    val struct = new Struct(schema)
    message.headers.foreach {
      case (key, data) =>
        struct.put(key, data.value)
    }
    struct
  }

  /**
    * Converts a schema to optional if it is not already optional without creating a wrapper around it
    * @param schema The schema to convert
    * @return The schema as optional
    */
  def toOptional(schema: Schema): Schema =
    if (schema.isOptional) schema
    else {
      schema.`type`() match {
        case Schema.Type.BOOLEAN => Schema.OPTIONAL_BOOLEAN_SCHEMA
        case Schema.Type.BYTES =>
          if (schema.name() == Decimal.LOGICAL_NAME) {
            val scale = schema.parameters().get(Decimal.SCALE_FIELD).toInt
            Decimal.builder(scale).optional().name(schema.name()).version(1)
              .defaultValue(schema.defaultValue())
              .doc(schema.doc()).build()
          } else Schema.OPTIONAL_BYTES_SCHEMA

        case Schema.Type.FLOAT32 => Schema.OPTIONAL_FLOAT32_SCHEMA
        case Schema.Type.FLOAT64 => Schema.OPTIONAL_FLOAT64_SCHEMA
        case Schema.Type.INT8    => Schema.OPTIONAL_INT8_SCHEMA
        case Schema.Type.INT16   => Schema.OPTIONAL_INT16_SCHEMA
        case Schema.Type.INT32 =>
          if (schema.name() == Date.LOGICAL_NAME) {
            var builder = Date.builder().optional()
            builder = builder.doc(schema.doc())
            builder = builder.defaultValue(schema.defaultValue())
            builder.build()
          } else if (schema.name() == Time.LOGICAL_NAME) {
            var builder = Time.builder().optional()
            builder = builder.doc(schema.doc())
            builder = builder.defaultValue(schema.defaultValue())
            builder.build()
          } else Schema.OPTIONAL_INT32_SCHEMA
        case Schema.Type.INT64 =>
          if (schema.name() == "org.apache.kafka.connect.data.Timestamp") {
            var builder = Timestamp.builder().optional()
            builder = builder.doc(schema.doc())
            builder = builder.defaultValue(schema.defaultValue())
            builder.build()
          } else
            Schema.OPTIONAL_INT64_SCHEMA
          Schema.OPTIONAL_INT64_SCHEMA
        case Schema.Type.STRING => Schema.OPTIONAL_STRING_SCHEMA
        case Schema.Type.ARRAY  => SchemaBuilder.array(schema.valueSchema()).optional().build()
        case Schema.Type.MAP    => SchemaBuilder.map(schema.keySchema(), schema.valueSchema()).optional().build()
        case Schema.Type.STRUCT =>
          val builder = SchemaBuilder.struct().optional()
          schema.fields().asScala.foldLeft(builder) {
            case (b, field) =>
              b.field(field.name(), field.schema())
          }.build()

      }
    }
}

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
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.cloud.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.formats.writer.ArraySinkData
import io.lenses.streamreactor.connect.cloud.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.formats.writer.SinkData
import io.lenses.streamreactor.connect.cloud.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.model.Topic
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * When JSON converter is used the Connect framework does not provide the schema  for payloads like Array[?] of Map[String, ?].
  * For all the other payload types, there is no transformation.
  * This class is responsible for adapting the message payload to one with a schema.
  */
class AddConnectSchemaTransformer(topic: Topic, settings: DataStorageSettings) extends Transformer {
  override def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] =
    if (topic != message.topic) {
      Left(
        new RuntimeException(
          s"Invalid state reached. Schema enrichment transformer topic [${topic.value}] does not match incoming message topic [${message.topic.value}].",
        ),
      )
    } else {
      if (settings.hasEnvelope && (settings.key || settings.value)) {
        for {
          key <- if (settings.key) AddConnectSchemaTransformer.qualifyWithSchema(message.key) else message.key.asRight
          value <- if (settings.value) AddConnectSchemaTransformer.qualifyWithSchema(message.value)
          else message.value.asRight
        } yield {
          if ((key eq message.key) && (value eq message.value)) message
          else
            message.copy(key = key, value = value)
        }

      } else {
        message.asRight
      }
    }

}

object AddConnectSchemaTransformer {

  private def qualifyWithSchema(v: SinkData): Either[RuntimeException, SinkData] =
    v.schema().fold {
      convert(v.value).flatMap {
        case None => v.asRight
        case Some(schemaAndValue) =>
          schemaAndValue.value() match {
            case s:     Struct            => StructSinkData(s).asRight
            case jList: java.util.List[_] => ArraySinkData(jList, Some(schemaAndValue.schema())).asRight
            case _ => v.asRight
          }
      }
    } { _ =>
      v.asRight[RuntimeException]
    }

  def convert(map: java.util.Map[_, _]): Either[RuntimeException, Option[Struct]] =
    if (map == null) None.asRight
    else if (map.isEmpty) None.asRight
    else {
      //extract the Key type from the map
      val nonStringKey =
        map.asScala.map(_._1.getClass).filter(_ != classOf[String]).headOption

      nonStringKey match {
        case Some(keyType) =>
          Left(
            new IllegalArgumentException(
              s"Conversion not supported for type [${keyType.getName}]. Input is a Map with non-String keys.",
            ),
          )
        case None =>
          // order the map to get the same order of fields in the struct
          map.asInstanceOf[java.util.Map[String, _]].asScala.toList.sortBy(_._1)
            .traverse {
              case (k, v) =>
                convert(v).flatMap(_.map { sv =>
                  k -> sv
                }.asRight[RuntimeException])
            }.map(_.flatten)
            .flatMap {
              case Nil => None.asRight
              case keysAndValues =>
                val builder = keysAndValues.foldLeft(SchemaBuilder.struct()) {
                  case (builder, (k, v)) =>
                    builder.field(k, v.schema())
                }
                val struct =
                  keysAndValues.foldLeft(new Struct(builder.build())) {
                    case (struct, (k, v)) =>
                      struct.put(k, v.value())
                  }
                struct.some.asRight
            }
      }
    }

  def convert(list: java.util.List[_]): Either[RuntimeException, Option[SchemaAndValue]] =
    if (list == null) None.asRight
    else if (list.isEmpty) {
      None.asRight
    } else {
      val sList = list.asScala.toList
      sList.traverse(convert)
        .map(_.flatten)
        .flatMap { l =>
          //validate all types are the same
          val types = l.map(_.schema().`type`())
          if (types.distinct.size > 1) {
            Left(new IllegalArgumentException(s"Conversion not supported for type ${types.distinct}."))
          } else {
            if (types.head == Schema.Type.INT8) {
              Right(Some(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, l.map(_.value()).asJava)))
            } else {
              Right(Some(new SchemaAndValue(SchemaBuilder.array(l.head.schema()).optional().build(),
                                            l.map(_.value()).asJava,
              )))
            }
          }
        }
    }

  def convertArray(array: Array[_]): Either[RuntimeException, Option[SchemaAndValue]] =
    if (array == null) None.asRight
    else if (array.isEmpty) {
      None.asRight
    } else {
      array.toList.traverse(convert)
        .map(_.flatten)
        .flatMap { l =>
          //validate all types are the same
          val types = l.map(_.schema().`type`())
          if (types.distinct.size > 1) {
            Left(new IllegalArgumentException(s"Conversion not supported for type ${types.distinct}."))
          } else {
            if (types.head == Schema.Type.INT8) {
              val bytes  = l.map(_.value()).collect { case b: java.lang.Byte => b.byteValue() }.toArray
              val buffer = ByteBuffer.wrap(bytes)

              Right(Some(new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, buffer)))
            } else {
              Right(Some(new SchemaAndValue(SchemaBuilder.array(l.head.schema()).optional().build(),
                                            l.map(_.value()).asJava,
              )))
            }
          }
        }
    }

  private def convert(
    value: Any,
  ): Either[RuntimeException, Option[SchemaAndValue]] =
    if (value == null) {
      Right(None)
    } else {
      Option(value).collect {
        case v: java.util.Map[_, _] =>
          convert(v).flatMap(_.map(s => new SchemaAndValue(s.schema(), s)).asRight[RuntimeException])

        case v: java.util.List[_] => convert(v)
        case s: String =>
          new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, s).some.asRight[RuntimeException]
        case s: Short =>
          new SchemaAndValue(Schema.OPTIONAL_INT16_SCHEMA, s).some.asRight[RuntimeException]
        case b: Byte =>
          new SchemaAndValue(Schema.OPTIONAL_INT8_SCHEMA, b).some.asRight[RuntimeException]
        case v: Int =>
          new SchemaAndValue(Schema.OPTIONAL_INT32_SCHEMA, v).some.asRight[RuntimeException]
        case v: Long =>
          new SchemaAndValue(Schema.OPTIONAL_INT64_SCHEMA, v).some.asRight[RuntimeException]
        case v: Float =>
          new SchemaAndValue(Schema.OPTIONAL_FLOAT32_SCHEMA, v).some.asRight[RuntimeException]
        case v: Double =>
          new SchemaAndValue(Schema.OPTIONAL_FLOAT64_SCHEMA, v).some.asRight[RuntimeException]
        case v: Boolean =>
          new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, v).some.asRight[RuntimeException]
        case b: ByteBuffer =>
          new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, b).some.asRight[RuntimeException]
        case a: Array[_] => convertArray(a)
      } match {
        case None =>
          Left(new IllegalArgumentException(s"Conversion not supported for type ${value.getClass.getCanonicalName}."))
        case Some(value) => value
      }
    }
}

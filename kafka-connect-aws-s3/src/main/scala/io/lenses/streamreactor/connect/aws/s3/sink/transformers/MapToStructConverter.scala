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
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Converts a Map[String, Any] to a Kafka Connect Struct
  */
object MapToStructConverter {
  def convert(map: java.util.Map[_, _]): Either[RuntimeException, Option[Struct]] =
    if (map.isEmpty) None.asRight
    else {

      //extract the Key type from the map
      val keyType = map.keySet().asScala.head.getClass
      if (keyType != classOf[String]) {
        Left(new IllegalArgumentException(
          s"Conversion not supported for type [$keyType]. Input is a Map with non-String keys.",
        ))
      } else {
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

      } match {
        case None        => Left(new IllegalArgumentException(s"Conversion not supported for type ${value.getClass}."))
        case Some(value) => value
      }
    }
}

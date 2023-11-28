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
package io.lenses.streamreactor.common.schemas

import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema

import scala.jdk.CollectionConverters.ListHasAsScala

object SchemaHelper {
  implicit final class SchemaExtensions(val schema: Schema) extends AnyVal {
    def extractSchema(path: String): Either[FieldSchemaExtractionError, Schema] = {
      val fields = path.split('.')
      val start: Either[FieldSchemaExtractionError, State] = Right(State(schema, Vector.empty))
      fields.foldLeft(start) {
        case (l @ Left(_), _) => l
        case (Right(state), field) =>
          state.schema.`type`() match {
            case Schema.Type.STRUCT | Schema.Type.MAP =>
              state.schema.extractField(field) match {
                case Some(value) => Right(state.copy(schema = value.schema(), path = state.path :+ field))
                case None =>
                  val path = (state.path :+ field).mkString(".")
                  val msg =
                    s"Field [$path] does not exist. Schema is [${schema.`type`()}]. Available Fields are [${schema.fields().asScala.map(_.name()).mkString(",")}]"
                  val finalMsg = if (path.endsWith("*")) {
                    s"$msg. Nested fields must be specified explicitly in the selected clause"
                  } else {
                    msg
                  }

                  Left(FieldSchemaExtractionError(path, finalMsg))
              }
            case other =>
              val path = state.path.mkString(".")
              Left(FieldSchemaExtractionError(path,
                                              s"Expecting a schema to be a structure but found [${other.getName}].",
              ))
          }
      }.map(_.schema)
    }

    def extractField(field: String): Option[Field] =
      Option(schema.field(field))
  }

  private final case class State(schema: Schema, path: Vector[String])
}

case class FieldSchemaExtractionError(path: String, msg: String)

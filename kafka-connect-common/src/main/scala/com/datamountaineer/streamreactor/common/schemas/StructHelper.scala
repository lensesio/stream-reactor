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

import io.lenses.streamreactor.common.schemas.SchemaHelper.SchemaExtensions
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

import scala.jdk.CollectionConverters.ListHasAsScala

object StructHelper {

  implicit final class StructExtension(val struct: Struct) extends AnyVal {

    def extractValueFromPath(
      path: String,
    ): Either[FieldValueExtractionError, Option[AnyRef]] = {
      val fields = path.split('.')
      val start: Either[FieldValueExtractionError, State] = Right(
        State(Some(struct), Vector.empty),
      )

      fields
        .foldLeft(start) {
          case (l @ Left(_), _) => l
          case (s @ Right(state), field) =>
            state.value.fold(
              s.asInstanceOf[Either[FieldValueExtractionError, State]],
            ) {
              case s: Struct =>
                s.fieldValue(field) match {
                  case Some(value) =>
                    Right.apply[FieldValueExtractionError, State](
                      state.copy(value = Some(value), path = state.path :+ field),
                    )
                  case None =>
                    val path = (state.path :+ field).mkString(".")
                    val msg =
                      s"Field [$path] does not exist. Available Fields are [${s.schema().fields().asScala.map(_.name()).mkString(",")}]"
                    Left.apply[FieldValueExtractionError, State](
                      FieldValueExtractionError(path, msg),
                    )
                }
              case other =>
                val path = state.path.mkString(".")
                Left.apply[FieldValueExtractionError, State](
                  FieldValueExtractionError(
                    path,
                    s"Expecting a a structure but found [$other].",
                  ),
                )
            }
        }
        .map(_.value)
    }

    def fieldValue(field: String): Option[AnyRef] =
      Option(struct.schema().field(field)).map { _ =>
        struct.get(field)
      }

    def ++(input: Struct): Struct = {

      val newFields = struct.schema().fields().asScala ++ input.schema().fields().asScala

      val builder = newFields.foldLeft(SchemaBuilder.struct()) { (builder, field) =>
        builder.field(field.name(), field.schema())
      }

      //make new struct with the new schema
      val output = new Struct(builder.build())

      //add the values
      struct.schema().fields().asScala.foreach(f => output.put(f.name(), struct.get(f.name())))
      input.schema().fields().asScala.foreach(f => output.put(f.name(), input.get(f.name())))
      output
    }

    // Reduce a schema to the fields specified
    def reduceSchema(
      schema:       Schema,
      fields:       Map[String, String] = Map.empty,
      ignoreFields: Set[String]         = Set.empty,
    ): Struct = {

      val extractFields: Map[String, String] = {
        if (fields.contains("*") || fields.isEmpty) {
          //all fields excluding ignored
          schema
            .fields()
            .asScala
            .filterNot(f => ignoreFields.contains(f.name()))
            .map(f => (f.name, f.name()))
        } else {
          //selected fields excluding ignored
          fields.view.filterKeys(k => !ignoreFields.contains(k)).toMap
        }
      }.toMap

      //find the field
      val newStruct = new Struct(newSchemaWithFields(extractFields, schema))
      addFieldValuesToStruct(struct, newStruct, extractFields)
      newStruct
    }

    private def addFieldValuesToStruct(oldStruct: Struct, newStruct: Struct, fields: Map[String, String]): Unit =
      fields
        .foreach {
          case (name, alias) =>
            oldStruct.extractValueFromPath(name) match {
              case Left(value)  => throw new ConnectException(value.msg)
              case Right(value) => newStruct.put(alias, value.orNull)
            }
        }

    //TODO: this should return an Either with the Schema as opposed to log the error
    private def newSchemaWithFields(fields: Map[String, String], schema: Schema): Schema = {
      val builder = SchemaBuilder.struct
      fields
        .foreach {
          case (name, alias) =>
            schema.extractSchema(name) match {
              case Left(value)  => throw new ConnectException(value.msg)
              case Right(value) => builder.field(alias, value)
            }
        }

      builder.build()
    }
  }

  private final case class State(value: Option[AnyRef], path: Vector[String])

}

case class FieldValueExtractionError(path: String, msg: String)

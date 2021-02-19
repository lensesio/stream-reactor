/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.schemas

import SchemaHelper.SchemaExtensions
import com.typesafe.scalalogging.Logger
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object StructHelper {
  private val logger = Logger(LoggerFactory.getLogger(getClass.getName))

  implicit final class StructExtension(val struct: Struct) extends AnyVal {

    def extractValueFromPath(
        path: String): Either[FieldValueExtractionError, Option[AnyRef]] = {
      val fields = path.split('.')
      val start: Either[FieldValueExtractionError, State] = Right(
        State(Some(struct), Vector.empty))

      fields
        .foldLeft(start) {
          case (l @ Left(_), _) => l
          case (s @ Right(state), field) =>
            state.value.fold(
              s.asInstanceOf[Either[FieldValueExtractionError, State]]) {
              case s: Struct =>
                s.fieldValue(field) match {
                  case Some(value) =>
                    Right.apply[FieldValueExtractionError, State](
                      state.copy(value = Some(value),
                                 path = state.path :+ field))
                  case None =>
                    val path = (state.path :+ field).mkString(".")
                    val msg =
                      s"Field [$path] does not exist. Available Fields are [${s.schema().fields().asScala.map(_.name()).mkString(",")}]"
                    Left.apply[FieldValueExtractionError, State](
                      FieldValueExtractionError(path, msg))
                }
              case other =>
                val path = state.path.mkString(".")
                Left.apply[FieldValueExtractionError, State](
                  FieldValueExtractionError(
                    path,
                    s"Expecting a a structure but found [$other]."))
            }
        }
        .map(_.value)
    }

    def fieldValue(field: String): Option[AnyRef] = {
      Option(struct.schema().field(field)).map { _ =>
        struct.get(field)
      }
    }

    def ++(input: Struct): Struct = {
      val builder = SchemaBuilder.struct()

      //get the original records fields and values and add to new schema
      val originalFieldsAndValues = struct
        .schema()
        .fields()
        .asScala
        .map(f => {
          builder.field(f.name(), f.schema())
          f.name() -> struct.get(f.name())
        })

      //get the input records fields and values and add to new schema
      val inputFieldsAndValues = input
        .schema()
        .fields()
        .asScala
        .map(f => {
          builder.field(f.name(), f.schema())
          f.name() -> input.get(f.name())
        })

      //make new struct with the new schema
      val output = new Struct(builder.build())
      //add the values
      originalFieldsAndValues.foreach{ case (name, value) => output.put(name, value)}
      inputFieldsAndValues.foreach{ case (name, value) => output.put(name, value)}
      output
    }

    // Reduce a schema to the fields specified
    def reduceSchema(schema: Schema,
                     fields: Map[String, String] = Map.empty,
                     ignoreFields: Set[String] = Set.empty): Struct = {

      val extractFields = if (fields.contains("*") || fields.isEmpty) {
        //all fields excluding ignored
        schema
          .fields()
          .asScala
          .filterNot(f => ignoreFields.contains(f.name()))
          .map(f => (f.name, f.name()))
          .toMap
      } else {
        //selected fields excluding ignored
        fields.filterKeys(k => !ignoreFields.contains(k))
      }

      //find the field
      val newStruct = new Struct(newSchemaWithFields(extractFields, schema))
      addFieldValuesToStruct(struct, newStruct, extractFields)
      newStruct
    }

    private def addFieldValuesToStruct(oldStruct: Struct,
                                       newStruct: Struct,
                                       fields: Map[String, String]): Unit = {
      fields
        .foreach {
          case (name, alias) =>
            oldStruct.extractValueFromPath(name) match {
              case Left(value)  => logger.error(value.msg)
              case Right(value) => newStruct.put(alias, value.orNull)
            }
        }
    }

    //TODO: this should return an Either with the Schema as opposed to log the error
    private def newSchemaWithFields(fields: Map[String, String],
                                    schema: Schema): Schema = {
      val builder = SchemaBuilder.struct
      fields
        .foreach {
          case (name, alias) =>
            schema.extractSchema(name) match {
              case Left(value)  => logger.error(value.msg)
              case Right(value) => builder.field(alias, value)
            }
        }

      builder.build()
    }
  }

  private final case class State(value: Option[AnyRef], path: Vector[String])

}

case class FieldValueExtractionError(path: String, msg: String)

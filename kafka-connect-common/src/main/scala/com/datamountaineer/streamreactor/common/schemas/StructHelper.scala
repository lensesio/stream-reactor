package com.datamountaineer.streamreactor.common.schemas

import com.datamountaineer.streamreactor.common.schemas.SchemaHelper.SchemaExtensions
import com.typesafe.scalalogging.Logger
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object StructHelper {

  implicit final class StructExtension(val struct: Struct) extends {

    val logger = Logger(LoggerFactory.getLogger(getClass.getName))

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
      struct
        .schema()
        .fields()
        .asScala
        .toList
        .foreach(f => builder.field(f.name(), f.schema()))
      input
        .schema()
        .fields()
        .asScala
        .toList
        .foreach(f => builder.field(f.name(), f.schema()))
      val output = new Struct(builder.build())
      addFieldValuesToStruct(
        struct,
        output,
        struct.schema().fields().asScala.map(f => (f.name(), f.name())).toMap)
      addFieldValuesToStruct(
        input,
        output,
        input.schema().fields().asScala.map(f => (f.name(), f.name())).toMap)
      output
    }

    // Reduce a schema to the fields specified
    def reduceSchema(schema: Schema,
                     fields: Map[String, String] = Map.empty,
                     ignoreFields: Set[String] = Set.empty): Struct = {

      val allFields = if (fields.contains("*") || fields.isEmpty) true else false

      if (allFields && ignoreFields.isEmpty) {
        struct
      } else {
        // filter the fields if for the value
        val extractFields = if (allFields) {
          schema
            .fields()
            .asScala
            .filterNot(f => ignoreFields.contains(f.name()))
            .map(f => (f.name, f.name()))
            .toMap
        } else {
          fields.filterNot { case (k, _) => ignoreFields.contains(k) }
        }
        val newStruct = new Struct(newSchemaWithFields(extractFields, schema))
        addFieldValuesToStruct(struct, newStruct, extractFields)
        newStruct
      }
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

package com.datamountaineer.streamreactor.common.schemas

import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema

import scala.collection.JavaConverters._

object SchemaHelper {
  implicit final class SchemaExtensions(val schema: Schema) extends AnyVal {
    def extractSchema(path: String): Either[FieldSchemaExtractionError, Schema] = {
      val fields = path.split('.')
      val start: Either[FieldSchemaExtractionError, State] = Right(State(schema, Vector.empty))
      fields.foldLeft(start) {
        case (l@Left(_), _) => l
        case (Right(state), field) =>
          state.schema.`type`() match {
            case Schema.Type.STRUCT | Schema.Type.MAP =>
              state.schema.extractField(field) match {
                case Some(value) => Right(state.copy(schema = value.schema(), path = state.path :+ field))
                case None =>
                  val path = (state.path :+ field).mkString(".")
                  val msg = s"Field [$path] does not exist. Schema is ${schema.`type`()}. Available Fields are [${schema.fields().asScala.map(_.name()).mkString(",")}]"
                  Left(FieldSchemaExtractionError(path, msg))
              }
            case other=>
              val path = state.path.mkString(".")
              Left(FieldSchemaExtractionError(path, s"Expecting a schema to be a structure but found [${other.getName}]."))
          }
      }.map(_.schema)
    }

    def extractField(field: String): Option[Field] = {
      Option(schema.field(field))
    }
  }

  private final case class State(schema: Schema, path: Vector[String])
}

case class FieldSchemaExtractionError(path: String, msg: String)
/*
 * Copyright 2017 Landoop.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.connect.sql

import com.landoop.sql.SqlContext
import com.landoop.sql.Field._
import org.apache.calcite.sql.SqlSelect
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object StructSchemaSql {

  implicit class SchemaSqlExtensions(val schema: Schema) extends AnyVal {

    def getFields(path: Seq[String]): Seq[Field] = {
      def navigate(current: Schema, parents: Seq[String]): Seq[Field] = {
        if (Option(parents).isEmpty || parents.isEmpty) {
          current.`type`() match {
            case Schema.Type.STRUCT => current.fields().asScala
            case Schema.Type.MAP => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} since it resolved to a Map($current)")
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        } else {
          current.`type`() match {
            case Schema.Type.STRUCT =>
              val field = Option(current.field(parents.head))
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${parents.head} in schema:$current"))
              navigate(field.schema(), parents.tail)
            case _ => throw new IllegalArgumentException(s"Can't select fields ${path.mkString(".")} from schema:$current ")
          }
        }
      }

      navigate(schema, path)
    }

    def fromPath(path: Seq[String]): Seq[Field] = {
      AvroSchemaExtension.fromPath(schema, path)
    }

    def copy(query: SqlSelect, flatten: Boolean): Schema = {
      if (!flatten) {
        com.landoop.sql.Field.from(query)
        implicit val kcqlContext = new SqlContext(com.landoop.sql.Field.from(query))
        copy
      }
      else {
        this.flatten(com.landoop.sql.Field.from(query))
      }
    }

    def copy()(implicit sqlContext: SqlContext): Schema = {
      AvroSchemaExtension.copy(schema, Vector.empty)
    }

    def flatten(fields: Seq[com.landoop.sql.Field]): Schema = {
      def allowOnlyStarSelection() = {
        fields match {
          case Seq(f) if f.name == "*" => schema
          case _ => throw new IllegalArgumentException(s"You can't select fields from schema:$schema")
        }
      }

      schema.`type`() match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flattent schema type:${schema.`type`()}")
        case Schema.Type.BOOLEAN | Schema.Type.BYTES |
             Schema.Type.FLOAT32 | Schema.Type.FLOAT64 |
             Schema.Type.INT8 | Schema.Type.INT16 |
             Schema.Type.INT32 | Schema.Type.INT64 |
             Schema.Type.STRING => allowOnlyStarSelection()

        case Schema.Type.STRUCT =>
          fields match {
            case Seq(f) if f.name == "*" => schema
            case _ => createRecordSchemaForFlatten(fields)
          }
      }
    }

    def copyProperties(from: Schema): Schema = {
      Option(from.parameters()).foreach(schema.parameters().putAll)
      schema
    }

    private def createRecordSchemaForFlatten(fields: Seq[com.landoop.sql.Field]): Schema = {
      var builder = SchemaBuilder.struct().name(schema.name())
        .doc(schema.doc())
      Option(schema.parameters()).map(builder.parameters)
      Option(schema.defaultValue()).map(builder.defaultValue)
      if (schema.isOptional) builder.optional()

      val fieldParentsMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
        if (buffer.contains(f.name)) {
          throw new IllegalArgumentException(s"You have defined the field ${
            if (f.hasParents) {
              f.parents.mkString(".") + "." + f.name
            } else {
              f.name
            }
          } more than once!")
        }
        buffer += f.name
        map + (key -> buffer)
      }

      val colsMap = collection.mutable.Map.empty[String, Int]

      def getNextFieldName(fieldName: String): String = {
        colsMap.get(fieldName).map { v =>
          colsMap.put(fieldName, v + 1)
          s"${fieldName}_${v + 1}"
        }.getOrElse {
          colsMap.put(fieldName, 0)
          fieldName
        }
      }

      fields.foreach {

        case field if field.name == "*" =>
          val siblings = fieldParentsMap.get(Option(field.parents).map(_.mkString(".")).getOrElse(""))
          Option(field.parents)
            .map { p =>
              val s = schema.fromPath(p)
                .headOption
                .getOrElse(throw new IllegalArgumentException(s"Can't find field ${p.mkString(".")} in schema:$schema"))
                .schema()

              s.`type`() match {
                case Schema.Type.STRUCT =>
                  if (!s.isOptional) s.fields().asScala.toSeq
                  else s.fields().asScala.map(AvroSchemaExtension.withOptional)
                case other => throw new IllegalArgumentException(s"Field selection ${p.mkString(".")} resolves to schema type:$other. Only RECORD type is allowed")
              }
            }
            .getOrElse {
              if (!schema.isOptional) schema.fields().asScala
              else schema.fields.asScala.map(AvroSchemaExtension.withOptional)
            }
            .withFilter { f =>
              siblings.collect { case s if s.contains(f.name()) => false }.getOrElse(true)
            }
            .foreach { f =>
              AvroSchemaExtension.checkAllowedSchemas(f.schema(), field)
              builder.field(getNextFieldName(f.name()), f.schema())
            }

        case field if field.hasParents =>
          schema.fromPath(field.parents.toVector :+ field.name)
            .foreach { extracted =>
              require(extracted != null, s"Invalid field:${(field.parents :+ field.name).mkString(".")}")
              AvroSchemaExtension.checkAllowedSchemas(extracted.schema(), field)
              if (field.alias == "*") {
                builder.field(getNextFieldName(extracted.name()), extracted.schema())
              } else {
                builder.field(getNextFieldName(field.alias), extracted.schema())
              }
            }

        case field =>
          val originalField = Option(schema.field(field.name))
            .getOrElse {
              throw new IllegalArgumentException(s"Can't find field:${field.name} in schema:$schema")
            }
          AvroSchemaExtension.checkAllowedSchemas(originalField.schema(), field)
          builder.field(getNextFieldName(field.alias), originalField.schema())
      }


      builder.build()
    }
  }

  private object AvroSchemaExtension {
    def copy(from: Schema, parents: Vector[String])(implicit kcqlContext: SqlContext): Schema = {
      from.`type`() match {
        case Schema.Type.STRUCT => createRecordSchema(from, parents)

        case Schema.Type.ARRAY =>
          val builder = SchemaBuilder.array(copy(from.valueSchema(), parents))
            .doc(from.doc())
            .name(from.name())
          Option(from.defaultValue()).foreach(builder.defaultValue)
          if (from.isOptional) builder.optional()

          builder.build()

        case Schema.Type.MAP =>
          val elementSchema = copy(from.valueSchema(), parents)
          val builder = SchemaBuilder.map(from.keySchema(), elementSchema)
            .name(from.name())
            .doc(from.doc())
          Option(from.defaultValue()).foreach(builder.defaultValue)

          if (from.isOptional) builder.optional()

          builder.build()

        case _ => from
      }
    }

    def copyAsOptional(from: Schema): Schema = {
      from.`type`() match {
        case Schema.Type.STRUCT =>
          val builder = SchemaBuilder.struct()
            .name(from.name())
            .doc(from.doc())
          Option(from.parameters()).foreach(builder.parameters)
          Option(from.defaultValue()).foreach(builder.defaultValue)

          from.fields().asScala.foreach(f => builder.field(f.name(), f.schema()))
          builder.optional().build()

        case Schema.Type.ARRAY =>
          val builder = SchemaBuilder.array(from.valueSchema())
            .doc(from.doc())
            .name(from.name())
          Option(from.defaultValue()).foreach(builder.defaultValue)
          builder.optional().build()

        case Schema.Type.MAP =>
          val builder = SchemaBuilder.map(from.keySchema(), from.valueSchema())
            .name(from.name())
            .doc(from.doc())
          Option(from.defaultValue()).foreach(builder.defaultValue)
          builder.optional().build()

        case otherType =>
          val builder = SchemaBuilder.`type`(otherType)
            .name(from.name())
            .doc(from.doc)
          Option(from.defaultValue()).foreach(builder.defaultValue)
          builder.optional().build()
      }
    }

    private def createRecordSchema(from: Schema, parents: Vector[String])(implicit sqlContext: SqlContext): Schema = {
      val builder = SchemaBuilder.struct()
        .name(from.name())
        .doc(from.doc())
      Option(from.parameters()).foreach(builder.parameters)
      Option(from.defaultValue()).foreach(builder.defaultValue)
      if (from.isOptional) builder.optional()

      val fields = sqlContext.getFieldsForPath(parents)
      fields match {
        case Seq() =>
          from.fields().asScala
            .foreach { schemaField =>
              val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
              builder.field(schemaField.name, newSchema)
            }

        case Seq(Left(f)) if f.name == "*" =>
          from.fields().asScala
            .foreach { schemaField =>
              val newSchema = copy(schemaField.schema(), parents :+ schemaField.name)
              builder.field(schemaField.name, newSchema)
            }

        case other =>
          fields.foreach {
            case Left(field) if field.name == "*" =>
              from.fields().asScala
                .withFilter(f => !fields.exists(e => e.isLeft && e.left.get.name == f.name))
                .map { f =>
                  val newSchema = copy(f.schema(), parents :+ f.name)
                  newSchema.copyProperties(f.schema())
                  builder.field(f.name(), newSchema)
                }

            case Left(field) =>
              val originalField = Option(from.field(field.name))
                .getOrElse {
                  throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}${field.name}. Schema doesn't contain it.")
                }
              val newSchema = copy(originalField.schema(), parents :+ field.name)
              newSchema.copyProperties(originalField.schema())
              builder.field(field.alias, newSchema)

            case Right(field) =>
              val originalField = Option(from.field(field))
                .getOrElse(throw new IllegalArgumentException(s"Invalid selecting ${parents.mkString("", ".", ".")}$field. Schema doesn't contain it."))
              val newSchema = copy(originalField.schema(), parents :+ field)
              newSchema.copyProperties(originalField.schema())
              builder.field(field, newSchema)
          }
      }

      builder.build
    }

    def fromPath(from: Schema, path: Seq[String]): Seq[Field] = {
      fromPathInternal(from, path, from.isOptional)
    }

    @tailrec
    private def fromPathInternal(from: Schema, path: Seq[String], isOptional: Boolean): Seq[Field] = {
      path match {
        case Seq(field) if field == "*" =>
          from.`type`() match {
            case Schema.Type.STRUCT =>
              if (!isOptional) from.fields().asScala
              else from.fields.asScala.map(withOptional)
            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case Seq(field) =>
          from.`type`() match {
            case Schema.Type.STRUCT =>
              if (!isOptional) Seq(from.field(field))
              else Seq(withOptional(from.field(field)))
            case other => throw new IllegalArgumentException(s"Can't select field:$field from ${other.toString}")
          }
        case head +: tail =>
          val next = Option(from.field(head))
            .getOrElse(throw new IllegalArgumentException(s"Can't find the field '$head'"))
          fromPathInternal(next.schema(), tail, isOptional || next.schema().isOptional)
      }
    }

    def withOptional(field: Field): Field = {
      new Field(field.name(), field.index(), copyAsOptional(field.schema()))
    }

    def checkAllowedSchemas(schema: Schema, field: com.landoop.sql.Field): Unit = {
      schema.`type`() match {
        case Schema.Type.ARRAY | Schema.Type.MAP => throw new IllegalArgumentException(s"Can't flatten from schema:$schema by selecting '${field.name}'")
        case _ =>
      }
    }
  }

}

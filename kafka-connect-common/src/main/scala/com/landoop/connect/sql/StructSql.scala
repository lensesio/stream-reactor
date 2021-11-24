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

import java.util

import com.landoop.connect.sql.StructSchemaSql._
import com.landoop.sql.{Field, SqlContext}
import org.apache.calcite.sql.SqlSelect
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object StructSql extends FieldValueGetter {

  implicit class IndexedRecordExtension(val struct: Struct) extends AnyVal {
    def get(fieldName: String): Any = {
      Option(struct.schema().field(fieldName))
        .map(f => struct.get(f.name()))
        .orNull
    }
  }

  implicit class StructSqlConverter(val from: Struct) extends AnyVal {
    def sql(query: String): Struct = {
      import org.apache.calcite.config.Lex
      import org.apache.calcite.sql.parser.SqlParser
      val config = SqlParser.configBuilder
        .setLex(Lex.MYSQL)
        .setCaseSensitive(false)
        .setIdentifierMaxLength(250)
        .build

      val withStructure: Boolean = query.trim.toLowerCase().endsWith("withstructure")
      val sql = if (withStructure) {
        query.trim.dropRight("withstructure".length)
      } else query

      val parser = SqlParser.create(sql, config)
      val select = Try(parser.parseQuery()) match {
        case Failure(e) => throw new IllegalArgumentException(s"Query is not valid.${e.getMessage}")
        case Success(sqlSelect: SqlSelect) => sqlSelect
        case Success(_) => throw new IllegalArgumentException("Only `select` statements are allowed")
      }
      this.sql(select, !withStructure)
    }

    def sql(query: SqlSelect, flatten: Boolean): Struct = {
      Option(from).map { _ =>
        if (!flatten) {
          implicit val kcqlContext = new SqlContext(Field.from(query))
          val schema = from.schema().copy()
          kcql(schema)
        } else {
          implicit val fields = Field.from(query)
          val schema = from.schema().flatten(fields)
          kcqlFlatten(schema)
        }
      }
    }.orNull

    def sql(fields: Seq[Field], flatten: Boolean): Struct = {
      Option(from).map { _ =>
        if (!flatten) {
          implicit val kcqlContext = new SqlContext(fields)
          val schema = from.schema()
          kcql(schema)
        } else {
          implicit val f = fields
          val schema = from.schema().flatten(fields)
          kcqlFlatten(schema)
        }
      }.orNull
    }

    def kcql(newSchema: Schema)(implicit sqlContext: SqlContext): Struct = {
      fromStruct(from, from.schema(), newSchema, Vector.empty[String])
    }


    def kcqlFlatten(newSchema: Schema)(implicit fields: Seq[Field]): Struct = {
      flattenStruct(from, newSchema)
    }


    def flattenStruct(struct: Struct, newSchema: Schema)(implicit fields: Seq[Field]): Struct = {
      val fieldsParentMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
        val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
        val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
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

      val newStruct = new Struct(newSchema)
      fields.zipWithIndex.foreach { case (field, index) =>
        if (field.name == "*") {
          val sourceFields = struct.schema().getFields(Option(field.parents).getOrElse(Seq.empty))
          val key = Option(field.parents).map(_.mkString(".")).getOrElse("")
          sourceFields
            .filter { f =>
              fieldsParentMap.get(key).forall(!_.contains(f.name()))
            }.foreach { f =>
            val extractedValue = get(struct, struct.schema(), Option(field.parents).getOrElse(Seq.empty[String]) :+ f.name())
            newStruct.put(getNextFieldName(f.name()), extractedValue.orNull)
          }
        }
        else {
          val extractedValue = get(struct, struct.schema(), Option(field.parents).getOrElse(Seq.empty[String]) :+ field.name)
          newStruct.put(getNextFieldName(field.alias), extractedValue.orNull)
        }
      }
      newStruct
    }


    def fromArray(value: Any,
                  schema: Schema,
                  targetSchema:
                  Schema,
                  parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      value match {
        case c: java.util.Collection[_] =>
          c.asScala.foldLeft(new java.util.ArrayList[Any](c.size())) { (acc, e) =>
            acc.add(from(e, schema.valueSchema(), targetSchema.valueSchema(), parents))
            acc
          }
        case other => throw new IllegalArgumentException(s"${other.getClass.getName} is not handled")
      }
    }

    def fromStruct(record: Struct,
                   schema: Schema,
                   targetSchema: Schema,
                   parents: Seq[String])(implicit sqlContext: SqlContext): Struct = {
      val fields = sqlContext.getFieldsForPath(parents)
      //.get(parents.head)
      val fieldsTuple = fields.headOption.map { _ =>
        fields.flatMap {
          case Left(field) if field.name == "*" =>
            val filteredFields = fields.collect { case Left(f) if f.name != "*" => f.name }.toSet

            schema.fields().asScala
              .withFilter(f => !filteredFields.contains(f.name()))
              .map { f =>
                val sourceField = Option(schema.field(f.name))
                  .getOrElse(throw new IllegalArgumentException(s"${f.name} was not found in $schema"))
                sourceField -> f
              }

          case Left(field) =>
            val sourceField = Option(schema.field(field.name))
              .getOrElse(throw new IllegalArgumentException(s"${field.name} can't be found in ${schema.fields.asScala.map(_.name).mkString(",")}"))

            val targetField = Option(targetSchema.field(field.alias))
              .getOrElse(throw new IllegalArgumentException(s"${field.alias} can't be found in ${targetSchema.fields.asScala.map(_.name).mkString(",")}"))

            List(sourceField -> targetField)

          case Right(field) =>
            val sourceField = Option(schema.field(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in $schema"))

            val targetField = Option(targetSchema.field(field))
              .getOrElse(throw new IllegalArgumentException(s"$field can't be found in ${targetSchema.fields.asScala.map(_.name).mkString(",")}"))

            List(sourceField -> targetField)

        }
      }.getOrElse {
        targetSchema.fields().asScala
          .map { f =>
            val sourceField = Option(schema.field(f.name))
              .getOrElse(throw new IllegalArgumentException(s"Can't find the field ${f.name} in ${schema.fields().asScala.map(_.name()).mkString(",")}"))
            sourceField -> f
          }
      }

      val newStruct = new Struct(targetSchema)
      fieldsTuple.foreach { case (sourceField, targetField) =>
        val v = from(record.get(sourceField.name()),
          sourceField.schema(),
          targetField.schema(),
          parents :+ sourceField.name)
        newStruct.put(targetField.name(), v)
      }
      newStruct
    }

    def fromMap(value: Any,
                fromSchema: Schema,
                targetSchema: Schema,
                parents: Seq[String])(implicit sqlContext: SqlContext): Any = {
      Option(value.asInstanceOf[java.util.Map[String, Any]]).map { map =>
        val newMap = new util.HashMap[String, Any]()
        //check if there are keys for this
        val fields = sqlContext.getFieldsForPath(parents)
        val initialMap = {
          if (fields.exists(f => f.isLeft && f.left.get.name == "*")) {
            map.keySet().asScala.map(k => k.toString -> k.toString).toMap
          } else {
            Map.empty[String, String]
          }
        }

        fields.headOption.map { _ =>
          fields.filterNot(f => f.isLeft && f.left.get.name != "*")
            .foldLeft(initialMap) {
              case (m, Left(f)) => m + (f.name -> f.alias)
              case (m, Right(f)) => m + (f -> f)
            }
        }
          .getOrElse(map.keySet().asScala.map(k => k.toString -> k.toString).toMap)
          .foreach { case (key, alias) =>
            Option(map.get(key)).foreach { v =>
              newMap.put(
                from(key, Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, null).asInstanceOf[String],
                from(v, fromSchema.valueSchema(), targetSchema.valueSchema(), parents))
            }
          }
        newMap
      }.orNull
    }

    def from(from: Any,
             fromSchema: Schema,
             targetSchema: Schema,
             parents: Seq[String])(implicit kcqlContext: SqlContext): Any = {
      Option(from).map { _ =>
        implicit val s = fromSchema
        fromSchema.`type`() match {
          case Schema.Type.BOOLEAN |
               Schema.Type.FLOAT64 | Schema.Type.FLOAT32 |
               Schema.Type.INT64 | Schema.Type.INT32 | Schema.Type.INT16 | Schema.Type.INT8 |
               Schema.Type.STRING | Schema.Type.BYTES => from

          case Schema.Type.ARRAY => fromArray(from, fromSchema, targetSchema, parents)

          case Schema.Type.MAP => fromMap(from, fromSchema, targetSchema, parents)

          case Schema.Type.STRUCT => fromStruct(from.asInstanceOf[Struct], fromSchema, targetSchema, parents)

          case other => throw new IllegalArgumentException(s"Invalid Avro schema type:$other")
        }
      }.orNull
    }
  }

}

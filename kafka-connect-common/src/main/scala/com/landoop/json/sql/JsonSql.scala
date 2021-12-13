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
package com.landoop.json.sql

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node._
import com.landoop.sql.{Field, SqlContext}
import org.apache.calcite.sql.SqlSelect

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object JsonSql {

  implicit class JsonSqlExtensions(val json: JsonNode) extends AnyVal {
    def sql(query: String): JsonNode = {
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
        case Success(sqlNode) => throw new IllegalArgumentException("Only `select` statements are allowed")
      }
      this.sql(select, !withStructure)
    }

    def sql(query: SqlSelect, flatten: Boolean): JsonNode = {
      sql(Field.from(query), flatten)
    }

    def sql()(implicit kcqlContext: SqlContext): JsonNode = {
      from(json, Vector.empty)
    }

    def sql(fields: Seq[Field], flatten: Boolean): JsonNode = {
      Option(json).map { _ =>
        fields match {
          case Seq() => json
          case Seq(f) if f.name == "*" => json
          case _ =>
            if (!flatten) {
              implicit val sqlContext = new SqlContext(fields)
              sql()
            } else {
              kcqlFlatten(fields)
            }
        }
      }.orNull
    }

    def kcqlFlatten(fields: Seq[Field]): JsonNode = {
      def addNode(source: JsonNode, target: ObjectNode, nodeName: String, select: String): Unit = {
        source match {
          case b: BinaryNode => target.put(nodeName, b.binaryValue())
          case b: BooleanNode => target.put(nodeName, b.booleanValue())
          case i: BigIntegerNode => target.put(nodeName, i.bigIntegerValue().longValue())
          case d: DecimalNode => target.put(nodeName, d.decimalValue())
          case d: DoubleNode => target.put(nodeName, d.doubleValue())
          case fl: FloatNode => target.put(nodeName, fl.floatValue())
          case i: IntNode => target.put(nodeName, i.intValue())
          case l: LongNode => target.put(nodeName, l.longValue())
          case s: ShortNode => target.put(nodeName, s.shortValue())
          case t: TextNode => target.put(nodeName, t.textValue())
          case o: ObjectNode => target.set(nodeName, o).asInstanceOf[ObjectNode]
          case _: NullNode =>
          case _: MissingNode =>
          case other => throw new IllegalArgumentException(s"Invalid path $select")
        }
      }

      fields match {
        case Seq() => json
        case Seq(f) if f.name == "*" => json
        case _ =>
          json match {
            case node: ObjectNode =>

              val fieldsParentMap = fields.foldLeft(Map.empty[String, ArrayBuffer[String]]) { case (map, f) =>
                val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
                val buffer = map.getOrElse(key, ArrayBuffer.empty[String])
                buffer += f.name
                map + (key -> buffer)
              }

              val newNode = new ObjectNode(JsonNodeFactory.instance)

              val fieldsNameMap = collection.mutable.Map.empty[String, Int]

              def getNextFieldName(name: String): String = {
                val count = fieldsNameMap.getOrElse(name, 0)
                fieldsNameMap += name -> (count + 1)
                if (count == 0) {
                  name
                } else {
                  s"${name}_$count"
                }
              }

              fields.foreach { f =>
                val sourceNode = {
                  if (!f.hasParents) {
                    Some(node)
                  } else {
                    Option(f.parents).flatMap(p => path(p))
                  }
                }

                val fieldPath = Option(f.parents).map(_.mkString(".") + "." + f.name).getOrElse(f.name)

                sourceNode match {
                  case None =>
                  case Some(on: ObjectNode) =>
                    if (f.name != "*") {
                      Option(on.get(f.name)).foreach { childNode =>
                        addNode(childNode, newNode, getNextFieldName(f.alias), fieldPath)
                      }
                    } else {
                      val key = Option(f.parents).map(_.mkString(".")).getOrElse("")
                      on.fieldNames().asScala.filter { name =>
                        fieldsParentMap.get(key).forall(!_.contains(name))
                      }.foreach { name =>
                        addNode(on.get(name), newNode, getNextFieldName(name), fieldPath)
                      }
                    }
                  case Some(other) =>
                    throw new IllegalArgumentException(s"Invalid field selection. '$fieldPath' resolves to node of type:${other.getNodeType}")
                }

              }
              newNode
            case _ => throw new IllegalArgumentException(s"Can't flatten a json type of ${json.getNodeType}")
          }
      }
    }

    def path(path: Seq[String]): Option[JsonNode] = {
      def navigate(node: JsonNode, parents: Seq[String]): Option[JsonNode] = {
        Option(node).flatMap { _ =>
          parents match {
            case head +: tail => navigate(node.get(head), tail)
            case _ => Some(node)
          }
        }
      }

      navigate(json, path)
    }
  }


  private def from(json: JsonNode, parents: Seq[String])(implicit kcqlContext: SqlContext): JsonNode = {
    def checkFieldsAndReturn() = {
      val fields = kcqlContext.getFieldsForPath(parents)
      require(fields.isEmpty || (fields.size == 1 && fields.head.isLeft && fields.head.left.get.name == "*"),
        s"You can't select a field from a ${json.getNodeType.toString}.")
      json
    }

    json match {
      case null => null
      case _: BinaryNode => checkFieldsAndReturn()
      case _: BooleanNode => checkFieldsAndReturn()
      case _: BigIntegerNode => checkFieldsAndReturn()
      case _: DecimalNode => checkFieldsAndReturn()
      case _: DoubleNode => checkFieldsAndReturn()
      case _: FloatNode => checkFieldsAndReturn()
      case _: IntNode => checkFieldsAndReturn()
      case _: LongNode => checkFieldsAndReturn()
      case _: ShortNode => checkFieldsAndReturn()
      case _: TextNode => checkFieldsAndReturn()
      case _: NullNode => checkFieldsAndReturn()
      case _: MissingNode => checkFieldsAndReturn()

      case node: ObjectNode => fromObjectNode(node, parents)
      case array: ArrayNode => fromArray(parents, array)
      //case pojoNode:POJONode=>
      case other => throw new IllegalArgumentException(s"Can't apply SQL over node of type:${other.getNodeType}")
    }
  }

  private def fromObjectNode(node: ObjectNode, parents: Seq[String])(implicit kcqlContext: SqlContext) = {
    val newNode = new ObjectNode(JsonNodeFactory.instance)
    val fields = kcqlContext.getFieldsForPath(parents)
    if (fields.nonEmpty) {
      fields.foreach {
        case Right(parent) => Option(node.get(parent)).foreach { node =>
          newNode.set(parent, from(node, parents :+ parent)).asInstanceOf[JsonNode]
        }
        case Left(parent) if parent.name == "*" =>
          node.fieldNames().asScala.withFilter { f =>
            !fields.exists {
              case Left(field) if field.name == f => true
              case _ => false
            }
          }.foreach { field =>
            newNode.set(field, from(node.get(field), parents :+ parent.name))
              .asInstanceOf[JsonNode]
          }

        case Left(parent) => Option(node.get(parent.name)).foreach { node =>
          newNode.set(parent.alias, from(node, parents :+ parent.name)).asInstanceOf[JsonNode]
        }
      }
    }
    else {
      node.fieldNames()
        .asScala.foreach { field =>
          newNode.set(
            field,
            from(node.get(field), parents :+ field)
          ).asInstanceOf[JsonNode]
        }
    }
    newNode
  }

  private def fromArray(parents: Seq[String], array: ArrayNode)(implicit kcqlContext: SqlContext): ArrayNode = {
    if (array.size() == 0) {
      array
    } else {
      val fields = kcqlContext.getFieldsForPath(parents)
      if (fields.size == 1 && fields.head.isLeft && fields.head.left.get.name == "*") {
        array
      } else {
        val newElements = array.elements().asScala.map(from(_, parents)).toList.asJava
        new ArrayNode(JsonNodeFactory.instance, newElements)
      }
    }
  }
}

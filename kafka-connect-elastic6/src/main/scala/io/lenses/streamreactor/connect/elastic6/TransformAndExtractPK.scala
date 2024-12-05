/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic6

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import io.lenses.json.sql.JsonSql._
import io.lenses.streamreactor.connect.json.SimpleJsonConverter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.header.Headers

import scala.util.Failure
import scala.util.Success
import scala.util.Try

private object TransformAndExtractPK extends StrictLogging {
  lazy val simpleJsonConverter = new SimpleJsonConverter()

  def apply(
    kcqlValues:    KcqlValues,
    schema:        Schema,
    value:         Any,
    withStructure: Boolean,
    keySchema:     Schema,
    key:           Any,
    headers:       Headers,
  ): (Option[JsonNode], Seq[Any]) =
    if (value == null) {
      (None, Seq.empty)
    } else {
      val result = for {
        jsonNode       <- extractJsonNode(value, schema)
        transformedJson = jsonNode.sql(kcqlValues.fields, !withStructure)
        keyJsonNodeOpt: Option[JsonNode] <- if (hasKeyFieldPath(kcqlValues.primaryKeysPath))
          extractOptionalJsonNode(key, keySchema)
        else Try(Option.empty[JsonNode])
      } yield {
        val primaryKeys = kcqlValues.primaryKeysPath.map { path =>
          extractPrimaryKey(path, jsonNode, keyJsonNodeOpt, headers)
        }
        (Option(transformedJson), primaryKeys)
      }

      result match {
        case Success(value) => value
        case Failure(e)     => throw e
      }
    }

  private def hasKeyFieldPath(paths: Seq[Vector[String]]): Boolean =
    paths.exists(_.head == KafkaMessageParts.Key)

  private def extractJsonNode(value: Any, schema: Schema): Try[JsonNode] =
    JsonPayloadExtractor.extractJsonNode(value, schema) match {
      case Left(error)       => Failure(new IllegalArgumentException(error))
      case Right(Some(node)) => Success(node)
      case Right(None)       => Failure(new IllegalArgumentException("Failed to extract JsonNode from value"))
    }

  private def extractOptionalJsonNode(value: Any, schema: Schema): Try[Option[JsonNode]] =
    if (value == null) Success(None)
    else {
      JsonPayloadExtractor.extractJsonNode(value, schema) match {
        case Left(error)    => Failure(new IllegalArgumentException(error))
        case Right(nodeOpt) => Success(nodeOpt)
      }
    }

  private def extractPrimaryKey(
    path:           Vector[String],
    jsonNode:       JsonNode,
    keyJsonNodeOpt: Option[JsonNode],
    headers:        Headers,
  ): Any =
    path.head match {
      case KafkaMessageParts.Key =>
        keyJsonNodeOpt match {
          case Some(keyNode) => PrimaryKeyExtractor.extract(keyNode, path.tail, "_key.")
          case None =>
            throw new IllegalArgumentException(
              s"Key path '${path.mkString(".")}' has a null value",
            )
        }
      case KafkaMessageParts.Value =>
        PrimaryKeyExtractor.extract(jsonNode, path.tail)
      case KafkaMessageParts.Header =>
        if (path.tail.size != 1) {
          throw new IllegalArgumentException(
            s"Invalid field selection for '${path.mkString(".")}'. " +
              s"Headers lookup only supports single-level keys. Nested header keys are not supported.",
          )
        }
        headers.lastWithName(path.tail.head) match {
          case null => throw new IllegalArgumentException(s"Header with key '${path.tail.head}' not found")
          case header => header.value() match {
              case value: String => value
              case null => throw new IllegalArgumentException(s"Header '${path.tail.head}' has a null value")
              case _    => throw new IllegalArgumentException(s"Header '${path.tail.head}' is not a string")
            }
        }
      case _ =>
        PrimaryKeyExtractor.extract(jsonNode, path)
    }
}

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
package io.lenses.streamreactor.connect.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.connect.data.Schema

trait SimpleJsonNode {

  def addToNode(key: JsonNode, value: JsonNode): Unit

  def get(): JsonNode
}

object SimpleJsonNode {

  def apply(map: Map[AnyRef, AnyRef], maybeSchema: Option[Schema]) =
    maybeSchema match {
      case Some(schema) if schema.keySchema.`type` == Schema.Type.STRING =>
        new ObjectJsonNode()
      case None if map.keySet.forall(s => s.isInstanceOf[String]) =>
        new ObjectJsonNode()
      case _ => new ArrayJsonNode()
    }
}

class ArrayJsonNode extends SimpleJsonNode {
  private val list = JsonNodeFactory.instance.arrayNode()
  override def addToNode(key: JsonNode, value: JsonNode): Unit = {
    list.add(JsonNodeFactory.instance.arrayNode.add(key).add(value))
    ()
  }

  override def get(): JsonNode = list
}

class ObjectJsonNode extends SimpleJsonNode {
  private val obj = JsonNodeFactory.instance.objectNode()
  override def addToNode(key: JsonNode, value: JsonNode): Unit = obj.set(key.asText, value)

  override def get(): JsonNode = obj
}

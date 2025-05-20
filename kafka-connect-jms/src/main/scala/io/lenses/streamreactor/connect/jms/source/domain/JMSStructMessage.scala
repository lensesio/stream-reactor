/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.jms.source.domain

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord

import java.util
import jakarta.jms._
import scala.jdk.CollectionConverters.EnumerationHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 11/03/2017.
  * stream-reactor
  */
object JMSStructMessage {
  val mapper                  = new ObjectMapper()
  val schema                  = getSchema()
  private val sourcePartition = Map.empty[String, String]
  private val offset          = Map.empty[String, String]

  def getSchema(): Schema =
    SchemaBuilder.struct().name("io.lenses.streamreactor.connect.jms")
      .field("message_timestamp", Schema.OPTIONAL_INT64_SCHEMA)
      .field("correlation_id", Schema.OPTIONAL_STRING_SCHEMA)
      .field("redelivered", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("reply_to", Schema.OPTIONAL_STRING_SCHEMA)
      .field("destination", Schema.OPTIONAL_STRING_SCHEMA)
      .field("message_id", Schema.OPTIONAL_STRING_SCHEMA)
      .field("mode", Schema.OPTIONAL_INT32_SCHEMA)
      .field("type", Schema.OPTIONAL_STRING_SCHEMA)
      .field("priority", Schema.OPTIONAL_INT32_SCHEMA)
      .field("bytes_payload", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("properties", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional())
      .build()

  def getStruct(target: String, message: Message): SourceRecord = {
    val struct = new Struct(schema)
      .put("message_timestamp", Option(message.getJMSTimestamp).getOrElse(null))
      .put("correlation_id", Option(message.getJMSCorrelationID).getOrElse(null))
      .put("redelivered", Option(message.getJMSRedelivered).getOrElse(null))
      .put("reply_to", Option(message.getJMSReplyTo).map(_.toString).orNull)
      .put("destination", Option(message.getJMSDestination).map(_.toString).orNull)
      .put("message_id", Option(message.getJMSMessageID).getOrElse(null))
      .put("mode", Option(message.getJMSDeliveryMode).getOrElse(null))
      .put("type", Option(message.getJMSType).getOrElse(null))
      .put("priority", Option(message.getJMSPriority).getOrElse(null))

    if (message.getPropertyNames.hasMoreElements) {
      struct.put("properties", getProperties(message))
    }
    struct.put("bytes_payload", getPayload(message))
    new SourceRecord(sourcePartition.asJava, offset.asJava, target, null, null, struct.schema(), struct)
  }

  def getProperties(message: Message): util.Map[String, String] =
    message.getPropertyNames.asScala.foldLeft(Map.empty[String, String])((acc, propertyName) =>
      acc + (propertyName.toString -> message.getStringProperty(propertyName.toString)),
    ).asJava

  def getPayload(message: Message): Array[Byte] =
    message match {
      case t: TextMessage => t.getText.getBytes
      case b: BytesMessage => {
        val length = b.getBodyLength.toInt
        val dest   = new Array[Byte](length)
        b.readBytes(dest, length)
        dest
      }
      case m: MapMessage => {
        val map   = scala.collection.mutable.Map[String, String]()
        val props = m.getMapNames
        while (props.hasMoreElements) {
          val name  = props.nextElement().toString
          val value = m.getStringProperty(name)
          map.put(name, value)
        }
        mapper.writeValueAsBytes(map)
      }
      case o: ObjectMessage => o.getObject().asInstanceOf[Array[Byte]]
    }
}

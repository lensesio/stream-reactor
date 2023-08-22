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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import org.apache.kafka.connect.data._

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Creates an envelope for the message detail. It is expected the Key and/or Value, if used to have a Connect schema attached.
  * @param settings The settings for the data storage for the topic
  */
case class SchemalessEnvelopeTransformer(topic: Topic, settings: DataStorageSettings) extends Transformer {
  def transform(message: MessageDetail): Either[RuntimeException, MessageDetail] =
    if (message.topic != topic) {
      Left(
        new RuntimeException(
          s"Invalid state reached. Envelope transformer topic [${topic.value}] does not match incoming message topic [${message.topic.value}].",
        ),
      )
    } else if (settings.isDataStored) {
      SchemalessEnvelopeTransformer.envelope(message, settings).asRight
    } else {
      message.asRight
    }

}

object SchemalessEnvelopeTransformer {

  /**
    * Creates an envelope schema for the message detail. This is a schema that contains the key, value and headers and metadata.
    * Key and Value schema is set optional to handle null data (i.e. deletes as tombstones)
    * {{{
    *   {
    *     "key": ...,
    *     "value": ...,
    *     "headers": {
    *       "header1": "value1",
    *       "header2": "value2"
    *     },
    *     "metadata": {
    *       "timestamp": 123456789,
    *       "topic": "topic1",
    *       "partition": 0,
    *       "offset": 1
    *
    *     }
    *   }
    * }}}
    *
    * @return
    */
  private def envelope(message: MessageDetail, settings: DataStorageSettings): MessageDetail = {
    val envelope = new java.util.HashMap[String, Any]()
    if (settings.key) envelope.put("key", convert(message.key))
    if (settings.value) envelope.put("value", convert(message.value))
    if (settings.metadata) envelope.put("metadata", metadataData(message))
    if (settings.headers) envelope.put("headers", headersData(message))

    message.copy(value = MapSinkData(envelope, None))
  }

  private def metadataData(message: MessageDetail): java.util.Map[String, Any] = {
    val metadata = new java.util.HashMap[String, Any]()
    message.timestamp.map(_.toEpochMilli).foreach(metadata.put("timestamp", _))
    metadata.put("topic", message.topic.value)
    metadata.put("partition", message.partition)
    metadata.put("offset", message.offset.value)
    metadata
  }

  private def headersData(message: MessageDetail): java.util.Map[String, Any] = {
    val headers = new java.util.HashMap[String, Any]()
    message.headers.foldLeft(headers) {
      case (map, (key, value)) =>
        map.put(key, convert(value))
        map
    }
  }

  def convert(data: SinkData): Any =
    data match {
      case StructSinkData(struct)      => convertStruct(struct)
      case MapSinkData(map, _)         => convertMap(map)
      case ArraySinkData(array, _)     => array.asScala.map(convert).asJava
      case ByteArraySinkData(array, _) => array
      case primitive: PrimitiveSinkData => primitive.value
      case _:         NullSinkData      => null
      case other => throw new IllegalArgumentException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }

  private def convertStruct(struct: Struct): java.util.Map[String, Any] = {
    val map = new java.util.HashMap[String, Any]()
    struct.schema().fields().asScala.foreach { field =>
      map.put(field.name(), convert(struct.get(field)))
    }
    map
  }
  private def convertMap(from: java.util.Map[_, _]): java.util.Map[Any, Any] =
    from.asScala.map { case (k, v) => convert(k) -> convert(v) }.asJava

  private def convert(any: Any): Any =
    any match {
      case map:   java.util.Map[_, _] => convertMap(map)
      case array: Array[_]            => array.map(convert)
      case list:  java.util.List[_]   => list.asScala.map(convert).asJava
      case s:     Struct              => convertStruct(s)
      case other => other
    }

}

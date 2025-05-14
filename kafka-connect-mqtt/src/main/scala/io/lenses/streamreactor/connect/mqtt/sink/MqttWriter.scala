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
package io.lenses.streamreactor.connect.mqtt.sink

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.connect.mqtt.config.MqttSinkSettings
import io.lenses.streamreactor.connect.mqtt.connection.MqttClientConnectionFn
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.paho.client.mqttv3.IMqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature

import scala.util.Either
import scala.util.Left
import scala.util.Right
import scala.util.Try
import java.util.{ Map => JMap }
import java.util.{ Collection => JCollection }
import java.util.{ HashMap => JHashMap }

/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */

object MqttWriter {
  def apply(settings: MqttSinkSettings): MqttWriter =
    new MqttWriter(MqttClientConnectionFn.apply(settings), settings)

  private val TargetFromFieldProperty = "mqtt.target.from.field"
}

class MqttWriter(client: IMqttClient, settings: MqttSinkSettings) extends StrictLogging with ErrorHandler {

  import MqttWriter.TargetFromFieldProperty

  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private val mappings: Map[String, Set[Kcql]] = settings.kcql.groupBy(k => k.getSource)

  private val jsonConverter = {
    val converter = new JsonConverter()
    val config    = new JHashMap[String, String]()
    config.put("schemas.enable", "false")
    converter.configure(config, false)
    converter
  }
  private val objectMapper = {
    val mapper = new ObjectMapper()
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    mapper
  }

  def write(records: Iterable[SinkRecord]): Either[Throwable, Unit] =
    records.foldLeft[Either[Throwable, Unit]](Right(())) { (acc, record) =>
      acc.flatMap { _ =>
        processRecord(record)
      }
    }

  private def processRecord(record: SinkRecord): Either[Throwable, Unit] =
    mappings.get(record.topic()) match {
      case Some(kcqls) =>
        kcqls.foldLeft[Either[Throwable, Unit]](Right(())) { (acc, k) =>
          acc.flatMap { _ =>
            for {
              target <- determineTarget(record, k)
              value  <- convertValue(record.value(), record.valueSchema())
              _      <- publishMessage(target, value)
            } yield ()
          }
        }
      case None => Right(())
    }

  private def determineTarget(record: SinkRecord, k: Kcql): Either[Throwable, String] =
    if (k.getTarget.toLowerCase == "_key") {
      Right(record.key().toString)
    } else if (k.getTarget.toLowerCase == "_topic") {
      Right(record.topic())
    } else {
      Option(k.getProperties.get(TargetFromFieldProperty)) match {
        case Some(value) if value.toLowerCase == "true" =>
          extractPathFromValue(record.value(), k.getTarget)
        case _ => Right(k.getTarget)
      }
    }

  private def extractPathFromValue(value: Any, path: String): Either[Throwable, String] =
    value match {
      case struct: Struct =>
        extractPathFromStruct(struct, path)
      case map: JMap[_, _] =>
        extractPathFromMap(map.asInstanceOf[JMap[String, Any]], path)
      case _ =>
        Left(new IllegalArgumentException(s"Cannot extract path from value of type ${value.getClass}"))
    }

  private def extractPathFromStruct(struct: Struct, path: String): Either[Throwable, String] =
    path.split("\\.").foldLeft[Either[Throwable, Any]](Right(struct)) { (acc, part) =>
      acc.flatMap {
        case s: Struct =>
          Try(s.get(part)).toEither.left.map {
            case e: DataException => new IllegalArgumentException(s"Path $part not found in struct: ${e.getMessage}")
            case e => e
          }
        case m: JMap[_, _] =>
          extractPathFromMap(m.asInstanceOf[JMap[String, Any]], part)
        case _ => Left(new IllegalArgumentException(s"Cannot extract path from non-struct/map value"))
      }
    }.map(_.toString)

  private def extractPathFromMap(map: JMap[String, Any], path: String): Either[Throwable, String] =
    path.split("\\.").foldLeft[Either[Throwable, Any]](Right(map)) { (acc, part) =>
      acc.flatMap {
        case m: JMap[_, _] =>
          Option(m.get(part)) match {
            case Some(value) => value match {
                case nestedMap:    JMap[_, _] => Right(nestedMap)
                case nestedStruct: Struct     => Right(nestedStruct)
                case other => Right(other)
              }
            case None => Left(new IllegalArgumentException(s"Path $part not found in map"))
          }
        case s: Struct =>
          extractPathFromStruct(s, part)
        case _ => Left(new IllegalArgumentException(s"Cannot extract path from non-map value"))
      }
    }.map(_.toString)

  private def convertValue(value: Any, schema: org.apache.kafka.connect.data.Schema): Either[Throwable, Array[Byte]] =
    value match {
      case bytes:  Array[Byte] => Right(bytes)
      case str:    String => Right(str.getBytes())
      case struct: Struct =>
        Try(jsonConverter.fromConnectData(null, struct.schema(), struct)).toEither
          .left.map(e => new Exception("Failed to convert struct to JSON", e))
      case map: JMap[_, _] if schema != null =>
        Try(jsonConverter.fromConnectData(null, schema, map)).toEither
          .left.map(e => new Exception("Failed to convert map to JSON", e))
      case map: JMap[_, _] =>
        Try(objectMapper.writeValueAsBytes(map)).toEither
          .left.map(e => new Exception("Failed to convert map to JSON", e))
      case coll: JCollection[_] if schema != null =>
        Try(jsonConverter.fromConnectData(null, schema, coll)).toEither
          .left.map(e => new Exception("Failed to convert collection to JSON", e))
      case coll: JCollection[_] =>
        Try(objectMapper.writeValueAsBytes(coll)).toEither
          .left.map(e => new Exception("Failed to convert collection to JSON", e))
      case other => Right(other.toString.getBytes())
    }

  private def publishMessage(target: String, value: Array[Byte]): Either[Throwable, Unit] =
    Try {
      val msg = new MqttMessage()
      msg.setQos(settings.mqttQualityOfService)
      msg.setPayload(value)
      msg.setRetained(settings.mqttRetainedMessage)
      client.publish(target, msg)
    }.toEither.left.map(e => new Exception(s"Failed to publish message to $target", e))

  def flush() = {}

  def close() = {
    client.disconnect()
    client.close()
  }
}

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
package io.lenses.streamreactor.connect.aws.s3.formats.reader.converters

import io.circe.Json
import io.circe.parser._
import io.lenses.streamreactor.connect.aws.s3.formats.reader.Converter
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.SourceWatermark
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord

import java.time.Instant
import scala.annotation.nowarn

/**
  * It expects the envelope payload to be a string with the following structure:
  *
  * {{{
  *  {
  *    "key": ...
  *    "value": ...
  *    "headers": {
  *    "header1": "value1",
  *    "header2": "value2"
  *    }
  *    metadata: {
  *    "timestamp": 1234567890,
  *    "topic": "my-topic",
  *    "partition": 0,
  *    "offset": 1234567890
  *    }
  *  }
  * }}}
  *
  * It will extract the key, value, headers and metadata and create a SourceRecord with the key, value and headers.
  * Both the key and value will be set to string types.
  * The metadata will be used to set the timestamp and target partition.
  *
  * The key, value, headers and metadata are expected to be optional. If they are missing it will set the value to null.
  *
  * @param watermarkPartition The watermark partition
  * @param topic The target topic
  * @param partition The target partition; only used if the envelope does not contain a partition
  * @param s3Location The S3 location of the object
  * @param lastModified The last modified date of the object
  */
class SchemalessEnvelopeConverter(
  watermarkPartition: java.util.Map[String, String],
  topic:              Topic,
  partition:          Integer,
  s3Location:         S3Location,
  lastModified:       Instant,
  instantF:           () => Instant = () => Instant.now(),
) extends Converter[String] {
  override def convert(envelope: String, index: Long): SourceRecord =
    //parse the json and then extract key,value, headers and metadata
    parse(envelope) match {
      case Left(value) => throw new RuntimeException(s"Failed to parse envelope [$envelope].", value)
      case Right(json) =>
        // expect value to be a json object
        val envelopeJson =
          json.asObject.getOrElse(throw new RuntimeException(s"Envelope [$envelope] is not a json object."))

        val keyIsArray   = envelopeJson("keyIsArray").isDefined
        val key          = envelopeJson("key").map(extractValue(_, keyIsArray))
        val valueIsArray = envelopeJson("valueIsArray").isDefined
        val value        = envelopeJson("value").map(extractValue(_, valueIsArray))
        val metadata = envelopeJson("metadata").map(
          _.asObject.getOrElse(throw new RuntimeException(s"Envelope [$envelope] does not contain metadata.")),
        )

        val sourceRecord = new SourceRecord(
          watermarkPartition,
          SourceWatermark.offset(s3Location, index, lastModified),
          topic.value,
          metadata.flatMap(j => j("partition").get.asNumber.flatMap(_.toInt).map(Integer.valueOf)).getOrElse(partition),
          key.map { _ =>
            if (!keyIsArray) Schema.OPTIONAL_STRING_SCHEMA
            else Schema.OPTIONAL_BYTES_SCHEMA
          }.orNull,
          key.orNull,
          value.map { _ =>
            if (!valueIsArray) Schema.OPTIONAL_STRING_SCHEMA
            else
              Schema.OPTIONAL_BYTES_SCHEMA
          }.orNull,
          value.orNull,
          metadata.flatMap(j => j("timestamp").get.asNumber.flatMap(_.toLong).map(Long.box)).getOrElse(
            instantF().toEpochMilli,
          ),
        )
        envelopeJson("headers")
          .map(
            _.asObject.getOrElse(throw new RuntimeException(s"Envelope [$envelope] does not contain headers.")),
          )
          .foreach { headersJson =>
            headersJson.toMap.foldLeft(sourceRecord.headers()) {
              case (headers, (k, v: Json)) =>
                v match {
                  case Json.Null  => headers.add(k, null)
                  case Json.True  => headers.addBoolean(k, true)
                  case Json.False => headers.addBoolean(k, false)
                  case _ if v.isNumber =>
                    val jNumber = v.asNumber.get
                    jNumber.toLong match {
                      case Some(long) => headers.addLong(k, long)
                      case _          => headers.addDouble(k, jNumber.toDouble)
                    }
                  case _ if v.isString => headers.addString(k, v.asString.get)
                  case _ if v.isArray =>
                    throw new RuntimeException(s"Header [$k] is an array. Only primitives are supported.")
                  case _ if v.isObject =>
                    throw new RuntimeException(s"Header [$k] is an object. Only primitives are supported.")
                  case _ => headers.addString(k, v.noSpaces)
                }
            }
          }
        sourceRecord

    }

  @nowarn
  private def extractValue(json: Json, isArray: Boolean) =
    json match {
      case Json.Null  => null
      case Json.True  => true
      case Json.False => false
      case _ if json.isNumber =>
        val jNumber = json.asNumber.get
        jNumber.toLong match {
          case Some(long) =>
            long
          case _ =>
            jNumber.toDouble
        }

      case _ if json.isString =>
        val s = json.asString.get
        if (isArray)
          //decode the string from base64
          java.util.Base64.getDecoder.decode(s)
        else
          s
      case _ if json.isArray =>
        json.noSpaces
      case _ if json.isObject =>
        json.noSpaces
      case _ => json.noSpaces
    }
}

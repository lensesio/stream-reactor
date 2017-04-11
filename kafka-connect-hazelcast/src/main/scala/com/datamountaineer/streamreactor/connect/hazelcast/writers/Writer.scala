/*
 * Copyright 2017 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.hazelcast.writers

import java.io.ByteArrayOutputStream

import com.datamountaineer.connector.config.FormatType
import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkSettings
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

/**
  * Created by andrew@datamountaineer.com on 02/12/2016. 
  * stream-reactor
  */
abstract class Writer(settings: HazelCastSinkSettings) extends ConverterUtil {

  def write(record: SinkRecord)

  def close

  /**
    * Convert a sink record to avro or Json string bytes
    *
    * @param record The sinkrecord to convert
    * @return an array of bytes
    **/
  def convert(record: SinkRecord): Object = {
    val storedAs = settings.format(record.topic())
    storedAs match {
      case FormatType.AVRO =>
        val avro = toAvro(record)
        serializeAvro(avro, avro.getSchema)
      case FormatType.JSON | FormatType.TEXT => toJson(record).toString
      case _ => throw new ConnectException(s"Unknown WITHFORMAT type ${storedAs.toString}")
    }
  }

  /**
    * Serialize an object to a avro encoded byte array
    *
    * @param datum  The object to serialize
    * @param schema The avro schema for the object
    * @return Avro encoded byte array.
    **/
  def serializeAvro(datum: Object, schema: Schema): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new ReflectDatumWriter[Object](schema)
    val encoder = EncoderFactory.get().binaryEncoder(out, null)
    out.reset()
    writer.write(datum, encoder)
    encoder.flush()
    out.toByteArray
  }

  /**
    * Convert sink records to json
    *
    * @param record A sink records to convert.
    **/
  def toJson(record: SinkRecord): JsonNode = {
    val extracted = convert(record, settings.fieldsMap(record.topic()), settings.ignoreFields(record.topic()))
    convertValueToJson(extracted)
  }

  /**
    * Convert sink records to avro
    *
    * @param record A sink records to convert.
    **/
  def toAvro(record: SinkRecord): GenericRecord = {
    val extracted = convert(record, settings.fieldsMap(record.topic()), settings.ignoreFields(record.topic()))
    convertValueToGenericAvro(extracted)
  }

  def buildPKs(record: SinkRecord): String = {
    val defaultKeys = s"${record.topic}-${record.kafkaPartition()}-${record.kafkaOffset()}"
    if (settings.pks(record.topic()).isEmpty) defaultKeys else settings.pks(record.topic()).mkString("-")
  }
}

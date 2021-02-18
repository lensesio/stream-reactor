/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.datamountaineer.streamreactor.common.converters.sink

import com.datamountaineer.streamreactor.common.converters.MsgKey
import io.confluent.connect.avro.AvroData

import java.io.ByteArrayOutputStream
import java.io.File
import org.apache.avro.{Schema => AvroSchema}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.ReflectDatumWriter
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException


class AvroConverter extends Converter {
  private val avroData = new AvroData(8)
  private var sinkToSchemaMap: Map[String, AvroSchema] = Map.empty
  private var avroWritersMap: Map[String, ReflectDatumWriter[Object]] = Map.empty

  override def convert(sinkTopic: String,
                       data: SinkRecord): SinkRecord = {
    Option(data) match {
      case None =>
        new SinkRecord(
          sinkTopic,
          0,
          null,
          null,
          avroData.toConnectSchema(sinkToSchemaMap(sinkTopic)),
          null,
          0
        )
      case Some(_) =>
        val kafkaTopic = data.topic()
        val writer = avroWritersMap.getOrElse(kafkaTopic.toLowerCase, throw new ConfigException(s"Invalid ${AvroConverter.SCHEMA_CONFIG} is not configured for $kafkaTopic"))

        val output = new ByteArrayOutputStream();
        val decoder = EncoderFactory.get().binaryEncoder(output, null)
        output.reset()

        val avro = avroData.fromConnectData(data.valueSchema(), data.value())
        avro.asInstanceOf[GenericRecord]

        val record = writer.write(avro, decoder)
        decoder.flush()
        val arr = output.toByteArray

        new SinkRecord(
          kafkaTopic,
          data.kafkaPartition(),
          MsgKey.schema,
          MsgKey.getStruct(sinkTopic, data.key().toString),
          data.valueSchema(),
          arr,
          0
        )


    }
  }

  override def initialize(config: Map[String, String]): Unit = {
    sinkToSchemaMap = AvroConverter.getSchemas(config)
    avroWritersMap = sinkToSchemaMap.map { case (key, schema) =>
      key -> new ReflectDatumWriter[Object](schema)
    }
  }
}

object AvroConverter {
  val SCHEMA_CONFIG = "connect.converter.avro.schemas"

  def getSchemas(config: Map[String, String]): Map[String, AvroSchema] = {
    config.getOrElse(SCHEMA_CONFIG, throw new ConfigException(s"[$SCHEMA_CONFIG] is not provided"))
      .split(';')
      .filter(_.trim.nonEmpty)
      .map(_.split("="))
      .map {
        case Array(sink, path) =>
          val file = new File(path)
          if (!file.exists()) {
            throw new ConfigException(s"Invalid [$SCHEMA_CONFIG]. The file [$path] doesn't exist!")
          }
          val s = sink.trim.toLowerCase()
          if (s.isEmpty) {
            throw new ConfigException(s"Invalid [$SCHEMA_CONFIG]. The topic is not valid for entry containing [$path]")
          }
          s -> new AvroSchema.Parser().parse(file)
        case _ => throw new ConfigException(s"[$SCHEMA_CONFIG] is not properly set. The format is Mqtt_Sink->AVRO_FILE")
      }.toMap
  }
}

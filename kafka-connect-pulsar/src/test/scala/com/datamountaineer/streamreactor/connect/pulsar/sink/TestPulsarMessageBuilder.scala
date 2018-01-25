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

package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSinkConfig, PulsarSinkSettings}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConverters._

class TestPulsarMessageBuilder extends WordSpec with Matchers with BeforeAndAfterAll with StrictLogging {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  def getSchema: Schema = {
    SchemaBuilder.struct
      .field("int8", SchemaBuilder.int8().defaultValue(2.toByte).doc("int8 field").build())
      .field("int16", Schema.INT16_SCHEMA)
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("float32", Schema.FLOAT32_SCHEMA)
      .field("float64", Schema.FLOAT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .build()
  }


  def getStruct(schema: Schema): Struct = {
    new Struct(schema)
      .put("int8", 12.toByte)
      .put("int16", 12.toShort)
      .put("int32", 12)
      .put("int64", 12L)
      .put("float32", 12.2f)
      .put("float64", 12.2)
      .put("boolean", true)
      .put("string", "foo")
  }


  "should create json messages singlePartition mode" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY =  1000"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val builder = PulsarMessageBuilder(settings)

    val schema = getSchema
    val struct = getStruct(schema)
    val record1 = new SinkRecord("kafka_topic", 0, null, null, schema, struct, 1)
    val messages = builder.create(List(record1))

    messages.head._1 shouldBe pulsarTopic
    messages.head._2.getData.map(_.toChar).mkString shouldBe "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\"}"
  }

  "should create json messages with key for key hash" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic WITHKEY(string) WITHPARTITIONER = CustomPartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val builder = PulsarMessageBuilder(settings)

    val schema = SchemaBuilder.struct.field("string", Schema.STRING_SCHEMA)
    val struct =  new Struct(schema).put("string", "landoop")

    val valueSchema = getSchema
    val valueStruct = getStruct(valueSchema)

    val record1 = new SinkRecord("kafka_topic", 0, schema, struct, valueSchema, valueStruct, 1)
    val messages = builder.create(List(record1))

    messages.head._1 shouldBe pulsarTopic
    messages.head._2.getKey shouldBe "{\"string\":\"landoop\"}"
    messages.head._2.getData.map(_.toChar).mkString shouldBe "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\"}"
  }

  "should create json message with key for round robin" in {
    val config = PulsarSinkConfig(Map(
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHKEY(string)  WITHPARTITIONER = RoundRobinPartition WITHDELAY = 1000"
    ).asJava)


    val settings = PulsarSinkSettings(config)
    val builder = PulsarMessageBuilder(settings)

    val schema = SchemaBuilder.struct.field("string", Schema.STRING_SCHEMA)
    val struct =  new Struct(schema).put("string", "landoop")

    val valueSchema = getSchema
    val valueStruct = getStruct(valueSchema)

    val record1 = new SinkRecord("kafka_topic", 0, schema, struct, valueSchema, valueStruct, 1)
    val messages = builder.create(List(record1))

    messages.head._1 shouldBe pulsarTopic
    messages.head._2.getKey shouldBe "{\"string\":\"landoop\"}"
    messages.head._2.getData.map(_.toChar).mkString shouldBe "{\"int8\":12,\"int16\":12,\"int32\":12,\"int64\":12,\"float32\":12.2,\"float64\":12.2,\"boolean\":true,\"string\":\"foo\"}"
  }
}
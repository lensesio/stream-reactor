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

package com.datamountaineer.streamreactor.connect.coap.sink

import com.datamountaineer.streamreactor.common.converters.sink.SinkRecordToJson
import com.datamountaineer.streamreactor.connect.coap.configs.CoapConstants
import com.datamountaineer.streamreactor.connect.coap.{Server, TestBase}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.eclipse.californium.core.CoapClient
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpec

import java.nio.ByteBuffer
import java.util
import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters.{MapHasAsJava, SetHasAsJava, SetHasAsScala}


/**
  * Created by andrew@datamountaineer.com on 29/12/2016.
  * stream-reactor
  */
class TestCoapSink extends AnyWordSpec  with BeforeAndAfter with TestBase with MockitoSugar {
  val server = new Server(SINK_PORT_SECURE, SINK_PORT_INSECURE, KEY_PORT_INSECURE)

  before { server.start() }
  after { server.stop() }

  "should start a CoapSink and Write an insecure Coap server" in {
    val props = getPropsInsecureSink
    val task = new CoapSinkTask


    val context = mock[SinkTaskContext]
    when(context.configs()).thenReturn(props)
    task.initialize(context)
    task.start(props)
    val records = getTestRecords(10)

    @nowarn
    val json = records.map(r => SinkRecordToJson(r, Map.empty, Map.empty))
    task.put(records.asJava)
    val client = new CoapClient(s"$SINK_URI_INSECURE/$RESOURCE_INSECURE")

    val response1 = client.get()
    json.contains(response1.getResponseText) shouldBe true
    val response2 = client.get()
    json.contains(response2.getResponseText) shouldBe true
    val response3 = client.get()
    json.contains(response3.getResponseText) shouldBe true
    val response4 = client.get()
    json.contains(response4.getResponseText) shouldBe true
    val response5 = client.get()
    json.contains(response5.getResponseText) shouldBe true
    val response6 = client.get()
    json.contains(response6.getResponseText) shouldBe true
    val response7 = client.get()
    json.contains(response7.getResponseText) shouldBe true
    val response8 = client.get()
    json.contains(response8.getResponseText) shouldBe true
    val response9 = client.get()
    json.contains(response9.getResponseText) shouldBe true
    val response10 = client.get()
    json.contains(response10.getResponseText) shouldBe true

    task.stop()
  }


  def getPropsInsecureSink: util.Map[String, String] = {
    Map(
      "topics" -> TOPIC,
      CoapConstants.COAP_KCQL->SINK_KCQL_INSECURE,
      CoapConstants.COAP_URI->SINK_URI_INSECURE
    ).asJava
  }

  //generate some test records
  def getTestRecords(nbr : Int = 1) : Set[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to nbr).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i.toLong, System.currentTimeMillis(), TimestampType.CREATE_TIME)
      })
    }).toSet
  }


  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = {
    ASSIGNMENT
  }


  //build a test record schema
  def createSchema: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("float_field", Schema.FLOAT32_SCHEMA)
      .field("float64_field", Schema.FLOAT64_SCHEMA)
      .field("boolean_field", Schema.BOOLEAN_SCHEMA)
      .field("byte_field", Schema.BYTES_SCHEMA)
      .build
  }


  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
      .put("float_field", 0.1.toFloat)
      .put("float64_field", 0.199999)
      .put("boolean_field", true)
      .put("byte_field", ByteBuffer.wrap("bytes".getBytes))
  }

}

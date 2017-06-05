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

package com.datamountaineer.streamreactor.connect.coap

import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch

import com.datamountaineer.streamreactor.connect.coap.configs.CoapConstants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.californium.core.coap.CoAP.{ResponseCode, Type}
import org.eclipse.californium.core.coap.{MediaTypeRegistry, OptionSet, Response}
import org.eclipse.californium.elements.{RawData, RawDataChannel}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
trait TestBase extends WordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "coap_test"
  val RESOURCE_SECURE = "secure"
  val RESOURCE_INSECURE = "insecure"
  val SOURCE_KCQL_INSECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_INSECURE"
  val SOURCE_KCQL_SECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_SECURE"
  val SINK_KCQL_INSECURE = s"INSERT INTO $RESOURCE_INSECURE SELECT * FROM $TOPIC"
  val SINK_KCQL_SECURE = s"INSERT INTO $RESOURCE_SECURE SELECT * FROM $TOPIC"
  val DTLS_PORT = 5684
  val PORT = 5683

  val SOURCE_PORT_SECURE = DTLS_PORT
  val SOURCE_PORT_INSECURE = PORT
  val SINK_PORT_SECURE: Int = DTLS_PORT + 1000
  val SINK_PORT_INSECURE: Int = PORT + 1000
  val DISCOVER_URI = s"coap://${CoapConstants.COAP_DISCOVER_IP4}:${SOURCE_PORT_INSECURE}"
  val SOURCE_URI_INSECURE = s"coap://localhost:$SOURCE_PORT_INSECURE"
  val SOURCE_URI_SECURE = s"coaps://localhost:$SOURCE_PORT_SECURE"

  val SINK_URI_INSECURE = s"coap://localhost:$SINK_PORT_INSECURE"
  val SINK_URI_SECURE = s"coaps://localhost:$SINK_PORT_SECURE"

  val KEYSTORE_PASS = "endPass"
  val TRUSTSTORE_PASS = "rootPass"

  //val TRUSTSTORE_PATH = System.getProperty("truststore")
  //val KEYSTORE_PATH = System.getProperty("keystore")
  val KEYSTORE_PATH: String =  getClass.getResource("/certs2/keyStore.jks").getPath
  val TRUSTSTORE_PATH: String = getClass.getResource("/certs2/trustStore.jks").getPath

  protected val PARTITION: Int = 12
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getPropsInsecure: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_INSECURE,
        CoapConstants.COAP_URI->SOURCE_URI_INSECURE
    ).asJava
  }

  def getPropsInsecureDisco: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_INSECURE,
      CoapConstants.COAP_URI->DISCOVER_URI
    ).asJava
  }


  def getPropsInsecureSink: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SINK_KCQL_INSECURE,
      CoapConstants.COAP_URI->SINK_URI_INSECURE
    ).asJava
  }


  def getPropsSinkSecure: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SINK_KCQL_SECURE,
      CoapConstants.COAP_URI->SINK_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_TRUST_CERTS->"root",
      CoapConstants.COAP_DTLS_BIND_PORT->"63367"
    ).asJava
  }

  def getTestSink: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SINK_KCQL_SECURE,
      CoapConstants.COAP_URI->SINK_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_TRUST_CERTS->"root",
      CoapConstants.COAP_DTLS_BIND_PORT->"63366"
    ).asJava
  }

  def getPropsSecure: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_TRUST_CERTS->"root",
      CoapConstants.COAP_DTLS_BIND_PORT->"63368"
    ).asJava
  }

  def getTestSourceProps: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_TRUST_CERTS->"root",
      CoapConstants.COAP_DTLS_BIND_PORT->"63369"
    ).asJava
  }

  def getPropsSecureMultipleKCQL: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->s"$SOURCE_KCQL_SECURE;$SOURCE_KCQL_INSECURE",
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH,
      CoapConstants.COAP_DTLS_BIND_PORT->"9998"
    ).asJava
  }

  def getPropsSecureKeyNotFound: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->"blah",
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH
    ).asJava
  }

  def getPropsSecureTrustNotFound: util.Map[String, String] = {
    Map(CoapConstants.COAP_KCQL->SOURCE_KCQL_SECURE,
      CoapConstants.COAP_URI->SOURCE_URI_SECURE,
      CoapConstants.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapConstants.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapConstants.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapConstants.COAP_TRUST_STORE_PATH->"blah"
    ).asJava
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

  def createSinkRecord(record: Struct, topic: String, offset: Long): SinkRecord = {
    new SinkRecord(topic, 1, Schema.STRING_SCHEMA, "key", record.schema(), record, offset)
  }

  //generate some test records
  def getTestRecords(nbr : Int = 1) : Set[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment

    assignment.flatMap(a => {
      (1 to nbr).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i, System.currentTimeMillis(), TimestampType.CREATE_TIME)
      })
    }).toSet
  }

  def getCoapResponse: Response = {
    val response = new Response(ResponseCode.CREATED)

    response.setLast(false)
    response.setRTT(1)
    response.setAcknowledged(false)
    response.setCanceled(false)
    response.setConfirmable(false)

    response.setDestinationPort(SOURCE_PORT_INSECURE)
    response.setDuplicate(false)
    response.setMID(1)
    response.setPayload("TEST PAYLOAD")
    response.setRejected(false)

    response.setTimedOut(false)
    response.setTimestamp(0)
    response.setToken("token".getBytes)
    response.setType(Type.NON)

    val options = new OptionSet()
    options.setAccept(1)
    options.setBlock1("b1".getBytes)
    options.setBlock2("b2".getBytes)
    options.setContentFormat(MediaTypeRegistry.TEXT_PLAIN)
    options.setLocationPath("loc_path")
    options.setLocationQuery("loc_query")
    options.setMaxAge(100)
    options.setObserve(2)
    options.setProxyUri("proxy_uri")
    options.setSize1(1)
    options.setSize2(2)
    options.setUriHost("uri_host")
    options.setUriPort(99)
    options.setUriPath("uri_path")
    options.setUriPort(99)
    options.setUriQuery("uri_query")
    options.addETag("TestETag".getBytes)

    response.setOptions(options)
    response
  }

  class Channel(latch: CountDownLatch) extends RawDataChannel() {

    @Override
    def receiveData(raw : RawData) {
      println(new String(raw.getBytes))
      latch.countDown()
    }
  }
}



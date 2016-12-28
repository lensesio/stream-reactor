package com.datamountaineer.streamreactor.connect.coap

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CountDownLatch

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSourceConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.eclipse.californium.core.coap.CoAP.{ResponseCode, Type}
import org.eclipse.californium.core.coap.{MediaTypeRegistry, OptionSet, Response}
import org.eclipse.californium.elements.{RawData, RawDataChannel}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.mutable
import scala.collection.JavaConverters._
/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
trait TestBase extends WordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "coap_test"
  val RESOURCE_SECURE = "secure"
  val RESOURCE_INSECURE = "insecure"
  val KCQL_INSECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_INSECURE"
  val KCQL_SECURE = s"INSERT INTO $TOPIC SELECT * FROM $RESOURCE_SECURE"
  val PORT_SECURE = Server.DTLS_PORT
  val PORT_INSECURE = Server.PORT
  val URI_INSECURE = s"coap://localhost:${PORT_INSECURE}"
  val URI_SECURE = s"coaps://localhost:${PORT_SECURE}"
  val KEYSTORE_PASS = "endPass"
  val TRUSTSTORE_PASS = "rootPass"
  val KEYSTORE_PATH =  getClass.getResource("/certs/keyStore.jks").getPath
  val TRUSTSTORE_PATH = getClass.getResource("/certs/truststore.jks").getPath

  protected val PARTITION: Int = 12
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getPropsUnsecure = {
    Map(CoapSourceConfig.COAP_KCQL->KCQL_INSECURE,
        CoapSourceConfig.COAP_URI->URI_INSECURE
    ).asJava
  }

  def getPropsSecure = {
    Map(CoapSourceConfig.COAP_KCQL->KCQL_SECURE,
      CoapSourceConfig.COAP_URI->URI_SECURE,
      CoapSourceConfig.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapSourceConfig.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapSourceConfig.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapSourceConfig.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH
    ).asJava
  }

  def getPropsSecureMultipleKCQL = {
    Map(CoapSourceConfig.COAP_KCQL->s"$KCQL_SECURE;$KCQL_INSECURE",
      CoapSourceConfig.COAP_URI->URI_SECURE,
      CoapSourceConfig.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapSourceConfig.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapSourceConfig.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapSourceConfig.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH
    ).asJava
  }

  def getPropsSecureKeyNotFound = {
    Map(CoapSourceConfig.COAP_KCQL->KCQL_SECURE,
      CoapSourceConfig.COAP_URI->URI_SECURE,
      CoapSourceConfig.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapSourceConfig.COAP_KEY_STORE_PATH->"blah",
      CoapSourceConfig.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapSourceConfig.COAP_TRUST_STORE_PATH->TRUSTSTORE_PATH
    ).asJava
  }

  def getPropsSecureTrustNotFound = {
    Map(CoapSourceConfig.COAP_KCQL->KCQL_SECURE,
      CoapSourceConfig.COAP_URI->URI_SECURE,
      CoapSourceConfig.COAP_KEY_STORE_PASS->KEYSTORE_PASS,
      CoapSourceConfig.COAP_KEY_STORE_PATH->KEYSTORE_PATH,
      CoapSourceConfig.COAP_TRUST_STORE_PASS->TRUSTSTORE_PASS,
      CoapSourceConfig.COAP_TRUST_STORE_PATH->"blah"
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

  def createSinkRecord(record: Struct, topic: String, offset: Long) = {
    new SinkRecord(topic, 1, Schema.STRING_SCHEMA, "key", record.schema(), record, offset)
  }

  //generate some test records
  def getTestRecords(nbr : Int = 1) : Set[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to nbr).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toSet
  }

  def getCoapResponse() = {
    val response = new Response(ResponseCode.CREATED)

    response.setLast(false)
    response.setRTT(1)
    response.setAcknowledged(false)
    response.setCanceled(false)
    response.setConfirmable(false)

    response.setDestinationPort(PORT_INSECURE)
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



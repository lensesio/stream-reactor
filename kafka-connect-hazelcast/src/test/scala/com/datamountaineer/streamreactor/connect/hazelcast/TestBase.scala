package com.datamountaineer.streamreactor.connect.hazelcast

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util

import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkConfig
import com.hazelcast.config.Config
import com.hazelcast.core.{Hazelcast, HazelcastInstance, Message, MessageListener}
import com.sun.xml.internal.ws.encoding.MtomCodec.ByteArrayBuffer
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
trait TestBase extends WordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "sink_test"
  val TABLE = "table1"
  val EXPORT_MAP=s"INSERT INTO $TABLE SELECT * FROM $TOPIC WITHFORMAT avro"
  val EXPORT_MAP_JSON=s"INSERT INTO $TABLE SELECT * FROM $TOPIC WITHFORMAT json"
  val EXPORT_MAP_SELECTION = s"INSERT INTO $TABLE SELECT a, b, c FROM $TOPIC"
  val EXPORT_MAP_IGNORED = s"INSERT INTO $TABLE SELECT * FROM $TOPIC IGNORE a"
  val GROUP_NAME = "unittest"
  val json = "{\"id\":\"sink_test-12-1\",\"int_field\":12,\"long_field\":12,\"string_field\":\"foo\",\"float_field\":0.1,\"float64_field\":0.199999,\"boolean_field\":true,\"byte_field\":\"Ynl0ZXM=\"}"

  protected val PARTITION: Int = 12
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getProps = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
    ).asJava
  }

  def getPropsJson = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
    ).asJava
  }

  def getPropsSelection = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP_SELECTION,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
    ).asJava
  }

  def getPropsIgnored = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP_IGNORED,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
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
}

class MessageListenerImplAvro extends MessageListener[Object] {

  var gotMessage = false
  var message : Option[GenericRecord] = None
  val schemaString =
    """
      |{
      |  "type" : "record",
      |  "name" : "record",
      |  "fields" : [ {
      |    "name" : "id",
      |    "type" : "string"
      |  }, {
      |    "name" : "int_field",
      |    "type" : "int"
      |  }, {
      |    "name" : "long_field",
      |    "type" : "long"
      |  }, {
      |    "name" : "string_field",
      |    "type" : "string"
      |  }, {
      |    "name" : "float_field",
      |    "type" : "float"
      |  }, {
      |    "name" : "float64_field",
      |    "type" : "double"
      |  }, {
      |    "name" : "boolean_field",
      |    "type" : "boolean"
      |  }, {
      |    "name" : "byte_field",
      |    "type" : "bytes"
      |  } ],
      |  "connect.version" : 1,
      |  "connect.name" : "record"
      |}
    """.stripMargin
  val schema2  = new org.apache.avro.Schema.Parser().parse(schemaString)


  def onMessage(m : Message[Object]) {
    System.out.println("Received: " + m.getMessageObject)
    val bytes = m.getMessageObject.asInstanceOf[Array[Byte]]
    message = Some(deserializeFromAvro(bytes))
    gotMessage = true
  }

  def deserializeFromAvro(avroBytes: Array[Byte]) : GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](schema2)
    val bais = new ByteArrayInputStream(avroBytes)
    val decoder = DecoderFactory.get().directBinaryDecoder(bais, null)
    reader.read(null, decoder)
  }
}


class MessageListenerImplJson extends MessageListener[Object] {

  var gotMessage = false
  var message : Option[String] = None

  def onMessage(m : Message[Object]) {
    System.out.println("Received: " + m.getMessageObject)
    message = Some(new String(m.getMessageObject.asInstanceOf[Array[Byte]]))
    gotMessage = true
  }
}

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

package com.datamountaineer.streamreactor.connect.hazelcast

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.util

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkConfigConstants}
import com.hazelcast.core.{Message, MessageListener}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 08/08/16. 
  * stream-reactor
  */
trait TestBase extends WordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "sink_test"
  val TABLE = "table1"
  val EXPORT_MAP=s"INSERT INTO ${TABLE}_avro SELECT * FROM $TOPIC WITHFORMAT avro"
  val EXPORT_MAP_RB=s"INSERT INTO ${TABLE}_rb SELECT * FROM $TOPIC WITHFORMAT json STOREAS RING_BUFFER"
  val EXPORT_MAP_JSON_QUEUE=s"INSERT INTO ${TABLE}_queue SELECT * FROM $TOPIC WITHFORMAT json STOREAS QUEUE"
  val EXPORT_MAP_JSON_SET=s"INSERT INTO ${TABLE}_set SELECT * FROM $TOPIC WITHFORMAT json STOREAS SET"
  val EXPORT_MAP_JSON_LIST=s"INSERT INTO ${TABLE}_list SELECT * FROM $TOPIC WITHFORMAT json STOREAS LIST"
  val EXPORT_MAP_MULTIMAP_DEFAULT_PK=s"INSERT INTO ${TABLE}_multi SELECT * FROM $TOPIC WITHFORMAT json STOREAS MULTI_MAP"
  val EXPORT_MAP_IMAP_DEFAULT_PK=s"INSERT INTO ${TABLE}_multi SELECT * FROM $TOPIC WITHFORMAT json STOREAS IMAP"
  val EXPORT_MAP_JSON_ICACHE=s"INSERT INTO ${TABLE}_icache SELECT * FROM $TOPIC WITHFORMAT json STOREAS ICACHE"

  val EXPORT_MAP_JSON=s"INSERT INTO $TABLE SELECT * FROM $TOPIC WITHFORMAT json"
  val EXPORT_MAP_SELECTION = s"INSERT INTO $TABLE SELECT a, b, c FROM $TOPIC"
  val EXPORT_MAP_IGNORED = s"INSERT INTO $TABLE SELECT * FROM $TOPIC IGNORE a"
  val TESTS_GROUP_NAME = "dev"
  val json = "{\"id\":\"sink_test-12-1\",\"int_field\":12,\"long_field\":12,\"string_field\":\"foo\",\"float_field\":0.1,\"float64_field\":0.199999,\"boolean_field\":true,\"byte_field\":\"Ynl0ZXM=\"}"

  protected val PARTITION: Int = 12
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getProps = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsRB = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_RB,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonQueue = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON_QUEUE,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonSet = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON_SET,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonList = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON_LIST,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonMultiMapDefaultPKS = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_MULTIMAP_DEFAULT_PK,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonICache = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON_ICACHE,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJsonMapDefaultPKS = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_IMAP_DEFAULT_PK,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
    ).asJava
  }

  def getPropsJson = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_JSON,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"

    ).asJava
  }

  def getPropsSelection = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_SELECTION,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
        HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"

    ).asJava
  }

  def getPropsIgnored = {
    Map(HazelCastSinkConfigConstants.EXPORT_ROUTE_QUERY->EXPORT_MAP_IGNORED,
      HazelCastSinkConfigConstants.GROUP_NAME->TESTS_GROUP_NAME,
      HazelCastSinkConfigConstants.CLUSTER_MEMBERS->"localhost"
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
  def getTestRecords(nbr : Int = 1) : Seq[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to nbr).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i, System.currentTimeMillis(), TimestampType.CREATE_TIME)
      })
    }).toSeq
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
    message = Some(m.getMessageObject.asInstanceOf[String])
    gotMessage = true
  }
}

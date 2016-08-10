package com.datamountaineer.streamreactor.connect.hazelcast

import java.nio.ByteBuffer
import java.util

import com.datamountaineer.streamreactor.connect.hazelcast.config.HazelCastSinkConfig
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
  val KUDU_MASTER = "127.0.0.1"
  val EXPORT_MAP=s"INSERT INTO $TABLE SELECT * FROM $TOPIC"
  val EXPORT_MAP_SELECTION = s"INSERT INTO $TABLE SELECT a, b, c FROM $TOPIC"
  val EXPORT_MAP_IGNORED = s"INSERT INTO $TABLE SELECT * FROM $TOPIC IGNORE a"
  val GROUP_NAME = "unittest"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  def getConfig = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
    ).asJava
  }

  def getConfigSelection = {
    Map(HazelCastSinkConfig.EXPORT_ROUTE_QUERY->EXPORT_MAP_SELECTION,
      HazelCastSinkConfig.SINK_GROUP_NAME->GROUP_NAME
    ).asJava
  }

  def getConfigIgnored = {
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

  def createSchema2: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("float_field", Schema.FLOAT32_SCHEMA)
      .field("float64_field", Schema.FLOAT64_SCHEMA)
      .field("boolean_field", Schema.BOOLEAN_SCHEMA)
      .field("int64_field", Schema.INT64_SCHEMA)
      .build
  }

  def createSchema3: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("float_field", Schema.FLOAT32_SCHEMA)
      .field("float64_field", Schema.FLOAT64_SCHEMA)
      .field("boolean_field", Schema.BOOLEAN_SCHEMA)
      .field("int64_field", Schema.INT64_SCHEMA)
      .field("new_field", Schema.STRING_SCHEMA)
      .build
  }

  def createSchema4: Schema = {
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
      .field("int64_field", SchemaBuilder.int64().defaultValue(20.toLong).build())
      .build
  }


  def createSchema5: Schema = {
    SchemaBuilder.struct.name("record")
      .version(2)
      .field("id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .field("float_field", Schema.FLOAT32_SCHEMA)
      .field("float64_field", Schema.FLOAT64_SCHEMA)
      .field("boolean_field", Schema.BOOLEAN_SCHEMA)
      .field("byte_field", Schema.BYTES_SCHEMA)
      .field("int64_field", SchemaBuilder.int64().defaultValue(20.toLong).build())
      .field("new_field", SchemaBuilder.string().defaultValue("").build())
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

  //build a test record
  def createRecord5(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
      .put("float_field", 0.1.toFloat)
      .put("float64_field", 0.199999)
      .put("boolean_field", true)
      .put("byte_field", ByteBuffer.wrap("bytes".getBytes))
      .put("int64_field", 12L)
      .put("new_field", "teststring")
  }

  def createSinkRecord(record: Struct, topic: String, offset: Long) = {
    new SinkRecord(topic, 1, Schema.STRING_SCHEMA, "key", record.schema(), record, offset)
  }

  //generate some test records
  def getTestRecords: Set[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 1).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toSet
  }
}
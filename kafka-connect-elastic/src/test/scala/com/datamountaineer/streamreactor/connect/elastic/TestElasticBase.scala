package com.datamountaineer.streamreactor.connect.elastic

import java.util
import java.util.UUID

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Struct, SchemaBuilder, Schema}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.io.File

trait TestElasticBase extends FunSuite with Matchers with BeforeAndAfter {
  val ELASTIC_SEARCH_HOSTNAMES = "localhost:9300"
  val TOPIC = "sink_test"
  var TMP : File = null

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  before {
    TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
  }

  after {
    TMP.deleteRecursively()
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
      .build
  }

  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
  }

  //generate some test records
  def getTestRecords: List[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 7).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }

  def getElasticSinkConfigProps = {
    Map (
      ElasticSinkConfig.URL->ELASTIC_SEARCH_HOSTNAMES,
      ElasticSinkConfig.ES_CLUSTER_NAME->ElasticSinkConfig.ES_CLUSTER_NAME_DEFAULT,
      ElasticSinkConfig.URL_PREFIX->ElasticSinkConfig.URL_PREFIX_DEFAULT
    ).asJava
  }

  def getElasticSinkConfigPropsDefaults = {
    Map (
      ElasticSinkConfig.URL->ELASTIC_SEARCH_HOSTNAMES
    ).asJava
  }
}
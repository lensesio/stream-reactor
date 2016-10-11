package com.datamountaineer.streamreactor.connect.rethink

import java.util

import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkSinkConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 21/06/16. 
  * stream-reactor-maven
  */
trait TestBase  extends WordSpec with Matchers with BeforeAndAfter {
  val TABLE = "rethink_table"
  val TOPIC = "rethink_topic"
  val ROUTE = s"INSERT INTO $TABLE SELECT * FROM $TOPIC"
  val DB = "test_db"
  val ROUTE_SELECT_UPSERT = s"UPSERT INTO $TABLE SELECT string_id, int_field FROM $TOPIC AUTOCREATE"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] =  new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getProps = {
    Map(ReThinkSinkConfig.EXPORT_ROUTE_QUERY->ROUTE,
      ReThinkSinkConfig.RETHINK_HOST->"localhost",
      ReThinkSinkConfig.RETHINK_DB->DB).asJava
  }

  def getPropsUpsertSelectRetry = {
    Map(ReThinkSinkConfig.EXPORT_ROUTE_QUERY->ROUTE_SELECT_UPSERT,
      ReThinkSinkConfig.RETHINK_HOST->"localhost",
      ReThinkSinkConfig.RETHINK_DB->DB,
      ReThinkSinkConfig.ERROR_POLICY->"RETRY").asJava
  }


  //build a test record schema
  def createSchema: Schema = {
    SchemaBuilder.struct.name("record")
      .version(1)
      .field("string_id", Schema.STRING_SCHEMA)
      .field("int_field", Schema.INT32_SCHEMA)
      .field("long_field", Schema.INT64_SCHEMA)
      .field("string_field", Schema.STRING_SCHEMA)
      .build
  }

  //build a test record
  def createRecord(schema: Schema, id: String): Struct = {
    new Struct(schema)
      .put("string_id", id)
      .put("int_field", 12)
      .put("long_field", 12L)
      .put("string_field", "foo")
  }

  def createMapPOJORecord(schema: Schema, id: String): java.util.Map[String, Object] = {
    val v = new util.HashMap[String, Object]()
    v.put("string_id", id)
    v.put("int_field", new java.lang.Integer(12))
    v.put("long_field", new java.lang.Long("12"))
    v
  }

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = {
    ASSIGNMENT
  }


  def generateTestRecords(schema: Schema, recordFn: (Schema, String) => Object): List[SinkRecord]= {
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 1).map(i => {
        val record = recordFn(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }

  //generate some test records
  def getTestRecords = generateTestRecords(createSchema, createRecord)
  def getMapPOJOTestRecords = generateTestRecords(null, createMapPOJORecord)
}

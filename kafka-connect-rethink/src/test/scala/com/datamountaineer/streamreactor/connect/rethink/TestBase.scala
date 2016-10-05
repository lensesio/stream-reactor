package com.datamountaineer.streamreactor.connect.rethink

import java.util

import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkConfig, ReThinkSourceConfig}
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
  val DB = "test"
  val ROUTE_SELECT_UPSERT = s"UPSERT INTO $TABLE SELECT string_id, int_field FROM $TOPIC AUTOCREATE"
  val IMPORT_ROUTE = s"INSERT INTO $TOPIC SELECT * FROM $TABLE initialize BATCH = 1000"
  val IMPORT_ROUTE_DELTA = s"INSERT INTO $TOPIC SELECT * FROM $TABLE BATCH = 1000"
  val IMPORT_ROUTE_2 = s"INSERT INTO $TOPIC SELECT * FROM $TABLE initialize BATCH = 1000;" +
    s"INSERT INTO ${TOPIC}_2 SELECT * FROM ${TABLE}_2 initialize BATCH = 1000"

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

  def getPropsSource = {
    Map(ReThinkSourceConfig.IMPORT_ROUTE_QUERY->IMPORT_ROUTE,
      ReThinkSourceConfig.RETHINK_HOST->"localhost",
      ReThinkSourceConfig.RETHINK_DB->DB).asJava
  }

  def getPropsSourceDelta = {
    Map(ReThinkSourceConfig.IMPORT_ROUTE_QUERY->IMPORT_ROUTE_DELTA,
      ReThinkSourceConfig.RETHINK_HOST->"localhost",
      ReThinkSourceConfig.RETHINK_DB->DB).asJava
  }

  def getPropsSource2 = {
    Map(ReThinkSourceConfig.IMPORT_ROUTE_QUERY->IMPORT_ROUTE_2,
      ReThinkSourceConfig.RETHINK_HOST->"localhost",
      ReThinkSourceConfig.RETHINK_DB->DB).asJava
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

  //get the assignment of topic partitions for the sinkTask
  def getAssignment: util.Set[TopicPartition] = {
    ASSIGNMENT
  }


  //generate some test records
  def getTestRecords: List[SinkRecord]= {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 1).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i)
      })
    }).toList
  }

}

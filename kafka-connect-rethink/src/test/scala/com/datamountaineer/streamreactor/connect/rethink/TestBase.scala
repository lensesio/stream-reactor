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

package com.datamountaineer.streamreactor.connect.rethink

import java.util

import com.datamountaineer.streamreactor.connect.rethink.config.ReThinkConfigConstants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 21/06/16. 
  * stream-reactor
  */
trait TestBase extends AnyWordSpec with Matchers with BeforeAndAfter {
  val TABLE = "rethink_table"
  val TOPIC = "rethink_topic"
  val BATCH_SIZE = 10
  val KCQL = s"INSERT INTO $TABLE SELECT * FROM $TOPIC BATCH = $BATCH_SIZE"
  val DB = "test"
  val ROUTE_SELECT_UPSERT = s"UPSERT INTO $TABLE SELECT string_id, int_field FROM $TOPIC AUTOCREATE BATCH = $BATCH_SIZE"
  val IMPORT_ROUTE = s"INSERT INTO $TOPIC SELECT * FROM $TABLE BATCH = $BATCH_SIZE initialize"
  val IMPORT_ROUTE_DELTA = s"INSERT INTO $TOPIC SELECT * FROM $TABLE BATCH = $BATCH_SIZE"
  val IMPORT_ROUTE_2: String = s"INSERT INTO $TOPIC SELECT * FROM $TABLE BATCH = $BATCH_SIZE initialize;" +
    s"INSERT INTO ${TOPIC}_2 SELECT * FROM ${TABLE}_2 initialize BATCH = $BATCH_SIZE initialize"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getProps: util.Map[String, String] = {
    Map(
      "topics" -> TOPIC,
      ReThinkConfigConstants.KCQL -> KCQL,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSource: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> IMPORT_ROUTE,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSourceDelta: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> IMPORT_ROUTE_DELTA,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSource2: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> IMPORT_ROUTE_2,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsUpsertSelectRetry: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> ROUTE_SELECT_UPSERT,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.RETHINK_DB -> DB,
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.ERROR_POLICY -> "RETRY").asJava
  }

  def getPropsConnTestNoAuth: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> IMPORT_ROUTE_2,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.RETHINK_DB -> DB,
      ReThinkConfigConstants.CERT_FILE->"cert.pem",
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey"
    ).asJava
  }

  def getPropsConnTestNoCert: util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> IMPORT_ROUTE_2,
      ReThinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkConfigConstants.RETHINK_DB -> DB,
      ReThinkConfigConstants.USERNAME->"admin",
      ReThinkConfigConstants.PASSWORD->"yourBrandNewKey",
      ReThinkConfigConstants.AUTH_KEY->"test"
    ).asJava
  }

  def test() : util.Map[String, String] = {
    Map(ReThinkConfigConstants.KCQL -> "INSERT INTO rethink-topic SELECT * FROM source_test",
    ReThinkConfigConstants.RETHINK_HOST -> "localhost",
    ReThinkConfigConstants.RETHINK_DB -> "test",
    ReThinkConfigConstants.USERNAME->"admin",
    ReThinkConfigConstants.PASSWORD->"yourBrandNewKey"
    //ReThinkConfigConstants.AUTH_KEY->"yourBrandNewKey",
    //ReThinkConfigConstants.CERT_FILE->"/Users/andrew/workspace/projects/datamountaineer/stream-reactor/cert.pem"
    ).asJava
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
  def getTestRecords: List[SinkRecord] = {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 1).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i, System.currentTimeMillis(), TimestampType.CREATE_TIME)
      })
    }).toList
  }

}

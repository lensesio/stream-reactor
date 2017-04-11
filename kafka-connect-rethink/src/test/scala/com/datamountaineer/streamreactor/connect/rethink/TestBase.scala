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

import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkConfigConstants, ReThinkSourceConfigConstants}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 21/06/16. 
  * stream-reactor-maven
  */
trait TestBase extends WordSpec with Matchers with BeforeAndAfter {
  val TABLE = "rethink_table"
  val TOPIC = "rethink_topic"
  val ROUTE = s"INSERT INTO $TABLE SELECT * FROM $TOPIC"
  val DB = "test"
  val ROUTE_SELECT_UPSERT = s"UPSERT INTO $TABLE SELECT string_id, int_field FROM $TOPIC AUTOCREATE"
  val IMPORT_ROUTE = s"INSERT INTO $TOPIC SELECT * FROM $TABLE initialize "
  val IMPORT_ROUTE_DELTA = s"INSERT INTO $TOPIC SELECT * FROM $TABLE"
  val IMPORT_ROUTE_2: String = s"INSERT INTO $TOPIC SELECT * FROM $TABLE initialize;" +
    s"INSERT INTO ${TOPIC}_2 SELECT * FROM ${TABLE}_2 initialize"

  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)

  def getProps: util.Map[String, String] = {
    Map(ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY -> ROUTE,
      ReThinkSinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkSinkConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSource: util.Map[String, String] = {
    Map(ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY -> IMPORT_ROUTE,
      ReThinkSourceConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkSourceConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSourceDelta: util.Map[String, String] = {
    Map(ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY -> IMPORT_ROUTE_DELTA,
      ReThinkSourceConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkSourceConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsSource2: util.Map[String, String] = {
    Map(ReThinkSourceConfigConstants.IMPORT_ROUTE_QUERY -> IMPORT_ROUTE_2,
      ReThinkSourceConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkSourceConfigConstants.RETHINK_DB -> DB).asJava
  }

  def getPropsUpsertSelectRetry: util.Map[String, String] = {
    Map(ReThinkSinkConfigConstants.EXPORT_ROUTE_QUERY -> ROUTE_SELECT_UPSERT,
      ReThinkSinkConfigConstants.RETHINK_HOST -> "localhost",
      ReThinkSinkConfigConstants.RETHINK_DB -> DB,
      ReThinkSinkConfigConstants.ERROR_POLICY -> "RETRY").asJava
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

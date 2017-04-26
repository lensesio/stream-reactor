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

package com.datamountaineer.streamreactor.connect.kudu

/**
  * Created by andrew@datamountaineer.com on 24/02/16. 
  * stream-reactor
  */

import java.nio.ByteBuffer
import java.util

import com.datamountaineer.streamreactor.connect.kudu.config.KuduSinkConfigConstants
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait TestBase extends WordSpec with BeforeAndAfter with Matchers {
  val TOPIC = "sink_test"
  val TABLE = "table1"
  val KUDU_MASTER = "127.0.0.1"
  val EXPORT_MAP = s"INSERT INTO $TABLE SELECT * FROM $TOPIC"
  val EXPORT_MAP_AUTOCREATE = EXPORT_MAP + " AUTOCREATE DISTRIBUTEBY name,adult INTO 10 BUCKETS"
  val EXPORT_MAP_AUTOCREATE_AUTOEVOLVE = EXPORT_MAP + " AUTOCREATE AUTOEVOLVE DISTRIBUTEBY name,adult INTO 10 BUCKETS"
  val schema =
    """
      |{ "type": "record",
      |"name": "Person",
      |"namespace": "com.datamountaineer",
      |"fields": [
      |{      "name": "name",      "type": "string"},
      |{      "name": "adult",     "type": "boolean"},
      |{      "name": "integer8",  "type": "int"},
      |{      "name": "integer16", "type": "int"},
      |{      "name": "integer32", "type": "long"},
      |{      "name": "integer64", "type": "long"},
      |{      "name": "float32",   "type": "float"},
      |{      "name": "float64",   "type": "double"}
      |]}"
    """.stripMargin
  val schemaDefaults =
    """
      |{ "type": "record",
      |"name": "Person",
      |"namespace": "com.datamountaineer",
      |"fields": [
      |{      "name": "name",      "type": "string"},
      |{      "name": "adult",     "type": "boolean"},
      |{      "name": "integer8",  "type": "int"},
      |{      "name": "integer16", "type": "int"},
      |{      "name": "integer32", "type": "long"},
      |{      "name": "integer64", "type": "long"},
      |{      "name": "float32",   "type": "float"},
      |{      "name": "float64",   "type": ["null", "double"], "default" : 10.00}
      |]}"
    """.stripMargin
  protected val PARTITION: Int = 12
  protected val PARTITION2: Int = 13
  protected val TOPIC_PARTITION: TopicPartition = new TopicPartition(TOPIC, PARTITION)
  protected val TOPIC_PARTITION2: TopicPartition = new TopicPartition(TOPIC, PARTITION2)
  protected val ASSIGNMENT: util.Set[TopicPartition] = new util.HashSet[TopicPartition]
  //Set topic assignments
  ASSIGNMENT.add(TOPIC_PARTITION)
  ASSIGNMENT.add(TOPIC_PARTITION2)

  before {

  }

  after {
  }

  def getConfig = {
    Map(KuduSinkConfigConstants.KUDU_MASTER -> KUDU_MASTER,
      KuduSinkConfigConstants.EXPORT_ROUTE_QUERY -> EXPORT_MAP,
      KuduSinkConfigConstants.ERROR_POLICY -> "THROW"
    ).asJava
  }

  def getConfigAutoCreate(url: String) = {
    Map(KuduSinkConfigConstants.KUDU_MASTER -> KUDU_MASTER,
      KuduSinkConfigConstants.EXPORT_ROUTE_QUERY -> EXPORT_MAP_AUTOCREATE,
      KuduSinkConfigConstants.ERROR_POLICY -> "THROW",
      KuduSinkConfigConstants.SCHEMA_REGISTRY_URL -> url
    ).asJava
  }

  def getConfigAutoCreateAndEvolve(url: String) = {
    Map(KuduSinkConfigConstants.KUDU_MASTER -> KUDU_MASTER,
      KuduSinkConfigConstants.EXPORT_ROUTE_QUERY -> EXPORT_MAP_AUTOCREATE_AUTOEVOLVE,
      KuduSinkConfigConstants.ERROR_POLICY -> "THROW",
      KuduSinkConfigConstants.SCHEMA_REGISTRY_URL -> url
    ).asJava
  }

  def getConfigAutoCreateRetry(url: String) = {
    Map(KuduSinkConfigConstants.KUDU_MASTER -> KUDU_MASTER,
      KuduSinkConfigConstants.EXPORT_ROUTE_QUERY -> EXPORT_MAP_AUTOCREATE,
      KuduSinkConfigConstants.ERROR_POLICY -> "RETRY",
      KuduSinkConfigConstants.SCHEMA_REGISTRY_URL -> url
    ).asJava
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
    new SinkRecord(topic, 1, Schema.STRING_SCHEMA, "key", record.schema(), record, offset, System.currentTimeMillis(), TimestampType.CREATE_TIME)
  }

  //generate some test records
  def getTestRecords: Set[SinkRecord] = {
    val schema = createSchema
    val assignment: mutable.Set[TopicPartition] = getAssignment.asScala

    assignment.flatMap(a => {
      (1 to 1).map(i => {
        val record: Struct = createRecord(schema, a.topic() + "-" + a.partition() + "-" + i)
        new SinkRecord(a.topic(), a.partition(), Schema.STRING_SCHEMA, "key", schema, record, i, System.currentTimeMillis(), TimestampType.CREATE_TIME)
      })
    }).toSet
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
}


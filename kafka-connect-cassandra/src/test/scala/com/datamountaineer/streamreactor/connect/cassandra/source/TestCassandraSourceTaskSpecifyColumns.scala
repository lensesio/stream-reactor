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

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.text.SimpleDateFormat
import java.util.Date

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.config.ConfigException
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._
import org.apache.kafka.connect.data.Schema

/**
 * test incremental mode specifying a subset of table columns
 * note: the timestamp column is required as part of SELECT
 */
class TestCassandraSourceTaskSpecifyColumns extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar with TestConfig
    with ConverterUtil {

  before {
    startEmbeddedCassandra()
  }

  after {
    stopEmbeddedCassandra()
  }

  "A Cassandra SourceTask should read records with only columns specified as Timestamp field" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val now = new Date()
    val formatted = formatter.format(now)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE3" +
      "(id, int_field, long_field, string_field, timestamp_field, timeuuid_field) " +
      s"VALUES ('id1', 2, 3, 'magic_string', '$formatted', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $TABLE3 PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE3",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.TIMESTAMP_TYPE -> "timestamp").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    //initialise the tasks context
    task.initialize(taskContext)
    //start task
    task.start(config)

    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(TABLE3) == 0) {
      Thread.sleep(5000)
    }

    //call poll again to drain the queue
    val records = task.poll()

    val sourceRecord = records.asScala.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    println(json)
    json.get("string_field").asText().equals("magic_string") shouldBe true
    json.get("timestamp_field").asText().size > 0
    json.get("int_field") shouldBe null
    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should read records with only columns specified" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $TABLE2 PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE2",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    //initialise the tasks context
    task.initialize(taskContext)
    //start task
    task.start(config)

    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(TABLE2) == 0) {
      Thread.sleep(5000)
    }

    //call poll again to drain the queue
    val records = task.poll()

    val sourceRecord = records.asScala.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    println(json)
    json.get("string_field").asText().equals("magic_string") shouldBe true
    json.get("timestamp_field").asText().size > 0
    json.get("int_field") shouldBe null
    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should read only columns specified and ignore those specified" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $TABLE2 IGNORE timestamp_field PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE2",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    //initialise the tasks context
    task.initialize(taskContext)
    //start task
    task.start(config)

    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(TABLE2) == 0) {
      Thread.sleep(5000)
    }

    //call poll again to drain the queue
    val records = task.poll()

    val sourceRecord = records.asScala.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    println(json)
    json.get("string_field").asText().equals("magic_string") shouldBe true
    json.get("int_field") shouldBe null
    json.get("timestamp_field") shouldBe null
    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should throw exception when timestamp column is not specified" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field FROM $TABLE2 PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE2",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000").asJava
    }

    val task = new CassandraSourceTask()
    task.initialize(taskContext)
    try {
      task.start(config)
      fail()
    } catch {
      case _: ConfigException => // Expected, so continue
    }
    task.stop()
  }

  "A Cassandra SourceTask should read only columns specified and use unwrap" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $TABLE2 IGNORE timestamp_field PK timestamp_field WITHUNWRAP",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE2",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    //initialise the tasks context
    task.initialize(taskContext)
    //start task
    task.start(config)

    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(TABLE2) == 0) {
      Thread.sleep(5000)
    }

    //call poll again to drain the queue
    val records = task.poll()

    // check the source record 
    val sourceRecord = records.asScala.head
    sourceRecord.keySchema shouldBe null
    sourceRecord.key shouldBe null
    sourceRecord.valueSchema shouldBe Schema.STRING_SCHEMA
    sourceRecord.value shouldBe "magic_string"

    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should read multiple columns and use unwrap" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.ROUTE_QUERY -> s"INSERT INTO sink_test SELECT string_field, long_field, timestamp_field FROM $TABLE2 IGNORE timestamp_field PK timestamp_field WITHUNWRAP",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE2",
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.POLL_INTERVAL -> "1000").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    //initialise the tasks context
    task.initialize(taskContext)
    //start task
    task.start(config)

    //trigger poll to have the readers execute a query and add to the queue
    task.poll()

    //wait a little for the poll to catch the records
    while (task.queueSize(TABLE2) == 0) {
      Thread.sleep(5000)
    }

    //call poll again to drain the queue
    val records = task.poll()

    // check the source record 
    val sourceRecord = records.asScala.head
    sourceRecord.keySchema shouldBe null
    sourceRecord.key shouldBe null
    sourceRecord.valueSchema shouldBe Schema.STRING_SCHEMA
    sourceRecord.value shouldBe "3,magic_string"

    //stop task
    task.stop()
  }
}



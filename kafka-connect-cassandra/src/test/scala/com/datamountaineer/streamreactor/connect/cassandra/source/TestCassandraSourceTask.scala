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
import java.util.{Date, UUID}
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.datastax.driver.core.Session
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/04/16.
  * stream-reactor
  */
@DoNotDiscover
class TestCassandraSourceTask extends WordSpec with Matchers with MockitoSugar with TestConfig
  with ConverterUtil with BeforeAndAfterAll {

  var session : Session = _
  val keyspace = "source"
  val contactPoint = "localhost"
  val userName = "cassandra"
  val password = "cassandra"

  override def beforeAll {
    session =  createTableAndKeySpace(keyspace ,secure = true, ssl = false)
  }

  override def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
  }

  "CassandraReader should read a tables records in incremental mode with timeuuid and time slices" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table " +
      s"(id text" +
      s", double_field double" +
      s", int_field int" +
      s", long_field bigint," +
      s" string_field text" +
      s", timestamp_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val props =  Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> s"INSERT INTO $TOPIC1 SELECT * FROM $table PK timestamp_field INCREMENTALMODE=timeuuid",
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    ).asJava

    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(props)

    // queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(session = session, setting = setting, context = taskContext, queue = queue)

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 111.1, 2, 3, 'magic_string', now());"
    session.execute(sql)

    // clear out the default of Jan 1, 1900
    // and read the inserted row
    reader.read()

    // sleep and check queue size
    while (queue.size() < 1) {
      Thread.sleep(1000)
    }
    queue.size() shouldBe 1

    // drain the queue
    val sourceRecord = QueueHelpers.drainQueue(queue, 1).asScala.toList.head
    val json: JsonNode = convertValueToJson(sourceRecord)
    json.get("string_field").asText().equals("magic_string") shouldBe true

    // insert another two records
    val sql2 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id2', 111.1, 3, 4, 'magic_string2', now());"
    session.execute(sql2)

    val sql3 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id3', 111.1,  4, 5, 'magic_string3', now());"
    session.execute(sql3)

    // sleep for longer than time slice (10 sec)
    Thread.sleep(11000)

    // insert another record
    val sql4 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 111.1, 4, 5, 'magic_string4', now());"
    session.execute(sql4)

    // sleep
    Thread.sleep(1000)

    //read
    reader.read()

    // sleep and check queue size
    // expecting to only get the 2 rows
    // in the time slice
    // the insert with "magic_string4" should fall
    // outside this range
    while (queue.size() < 2) {
      Thread.sleep(1000)
    }
    queue.size() shouldBe 2

    val sourceRecords2 = QueueHelpers.drainQueue(queue, 10).asScala.toList
    sourceRecords2.size shouldBe 2

    // sleep for longer than time slice (10 sec)
    Thread.sleep(11000)

    // read but don't insert any new rows
    reader.read()
    // sleep
    Thread.sleep(1000)
    //read
    reader.read()

    //sleep and check queue size
    while (queue.size() < 1) {
      Thread.sleep(1000)
    }
    //should be the inserted row "magic_string4"
    queue.size() shouldBe 1

    reader.close()
  }


  "CassandraReader should read a tables records in incremental mode as timeuuid" in {

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table " +
      s"(id text" +
      s", double_field double" +
      s", int_field int" +
      s", long_field bigint," +
      s" string_field text" +
      s", timestamp_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 111.1, 2, 3, 'magic_string', now());"
    session.execute(sql)

    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
      CassandraConfigConstants.KEY_SPACE -> keyspace,
      CassandraConfigConstants.USERNAME -> userName,
      CassandraConfigConstants.PASSWD -> password,
      CassandraConfigConstants.KCQL -> s"INSERT INTO $TOPIC1 SELECT * FROM $table PK timestamp_field INCREMENTALMODE=timeuuid",
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    ).asJava

    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(props)

    // queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(session = session, setting = setting, context = taskContext, queue = queue)
    reader.read()

    //sleep and check queue size
    while (queue.size() < 1) {
      Thread.sleep(1000)
    }
    queue.size() shouldBe 1

    //drain the queue
    val sourceRecords = QueueHelpers.drainQueue(queue, 1).asScala.toList
    val sourceRecord = sourceRecords.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    json.get("string_field").asText().equals("magic_string") shouldBe true

    //insert another two records
    val sql2 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id2', 111.1, 3, 4, 'magic_string2', now());"
    session.execute(sql2)

    val sql3 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id3', 111.1,  4, 5, 'magic_string3', now());"
    session.execute(sql3)

    // sleep
    Thread.sleep(1000)

    //read
    reader.read()

    //sleep and check queue size
    while (queue.size() < 2) {
      Thread.sleep(5000)
    }
    queue.size() shouldBe 2

    val sourceRecords2 = QueueHelpers.drainQueue(queue, 100).asScala.toList
    sourceRecords2.size shouldBe 2

    val struct = sourceRecords.head.value().asInstanceOf[Struct]
    //check a field
    struct.get("double_field") shouldBe 111.1

    //don't insert any new rows
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(1000)
    }

    //should be empty
    queue.size() shouldBe 0

    //insert another two records
    val sql4 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 111.1, 4, 5, 'magic_string4', now());"
    session.execute(sql4)

    Thread.sleep(2000)
    val sql5 = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 111.1, 5, 6, 'magic_string5', now());"
    session.execute(sql5)

    //read!!!!
    reader.read()

    //sleep and check queue size
    while (queue.size() < 2) {
      Thread.sleep(5000)
    }

    //should be 2!!!
    queue.size() shouldBe 2

    reader.close()
  }


  "A Cassandra SourceTask should read records with only columns specified as Timestamp field" in {

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ")
    val now = new Date()
    val formatted = formatter.format(now)

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table (" +
      s"id text" +
      s", int_field int" +
      s", long_field bigint" +
      s", string_field text" +
      s", timestamp_field timestamp" +
      s", timeuuid_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, int_field, long_field, string_field, timestamp_field, timeuuid_field) " +
      s"VALUES ('id1', 2, 3, 'magic_string', '$formatted', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $table PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$table",
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
    while (task.queueSize(table) == 0) {
      Thread.sleep(1000)
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
    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table " +
      s"(id text" +
      s", double_field double" +
      s", int_field int" +
      s", long_field bigint," +
      s" string_field text" +
      s", timestamp_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 111.1, 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $table IGNORE timestamp_field PK timestamp_field",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$table",
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
    while (task.queueSize(table) == 0) {
      Thread.sleep(1000)
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

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table " +
      s"(id text" +
      s", double_field double" +
      s", int_field int" +
      s", long_field bigint," +
      s" string_field text" +
      s", timestamp_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 111.1, 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(5000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> s"INSERT INTO sink_test SELECT string_field FROM $table PK timestamp_field INCREMENTALMODE=timeuuid",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$table",
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

    val table = "A" + UUID.randomUUID().toString.replace("-", "_")

    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$table " +
      s"(id text" +
      s", double_field double" +
      s", int_field int" +
      s", long_field bigint," +
      s" string_field text" +
      s", timestamp_field timeuuid" +
      s", PRIMARY KEY (id, timestamp_field)) WITH CLUSTERING ORDER BY (timestamp_field asc)")

    val sql = s"INSERT INTO $keyspace.$table" +
      "(id, double_field, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 111.1, 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> contactPoint,
        CassandraConfigConstants.KEY_SPACE -> keyspace,
        CassandraConfigConstants.USERNAME -> userName,
        CassandraConfigConstants.PASSWD -> password,
        CassandraConfigConstants.KCQL -> s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $table IGNORE timestamp_field PK timestamp_field WITHUNWRAP INCREMENTALMODE=timeuuid",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$table",
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
    while (task.queueSize(table) == 0) {
      Thread.sleep(1000)
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
}


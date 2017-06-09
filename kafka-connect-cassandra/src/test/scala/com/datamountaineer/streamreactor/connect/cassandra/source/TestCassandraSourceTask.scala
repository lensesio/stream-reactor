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


import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigSource, CassandraSettings}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 28/04/16.
  * stream-reactor
  */
class TestCassandraSourceTask extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar with TestConfig
  with ConverterUtil {

  before {
    startEmbeddedCassandra()
  }

  after {
    stopEmbeddedCassandra()
  }

  "A Cassandra SourceTask should start and read records from Cassandra" in {
    val session = createTableAndKeySpace(CASSANDRA_SOURCE_KEYSPACE, secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    //wait for cassandra write a little
    Thread.sleep(1000)

    val taskContext = getSourceTaskContextDefault
    //get config
    val config  = getCassandraConfigSourcePropsBulk
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

    //records.size() shouldBe(1)
    val sourceRecord = records.asScala.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    json.get("string_field").asText().equals("magic_string") shouldBe true
    //stop task
    task.stop()
  }

  "CassandraReader should read a tables records in incremental mode" in {
    val session =  createTableAndKeySpace(CASSANDRA_SOURCE_KEYSPACE ,secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id1', 2, 3, 'magic_string', now());"
    session.execute(sql)

    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(getCassandraConfigSourcePropsIncr)

    //queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(session = session, setting = setting, context = taskContext, queue = queue)
    reader.read()

    //sleep and check queue size
    while (queue.size() < 1) {
      Thread.sleep(5000)
    }
    queue.size() shouldBe 1

    //drain the queue
    val sourceRecords = QueueHelpers.drainQueue(queue, 1).asScala.toList
    val sourceRecord = sourceRecords.head
    //check a field
    val json: JsonNode = convertValueToJson(sourceRecord)
    json.get("string_field").asText().equals("magic_string") shouldBe true

    //insert another two records
    val sql2 = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id2', 3, 4, 'magic_string2', now());"
    session.execute(sql2)
    val sql3 = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id3', 4, 5, 'magic_string3', now());"
    session.execute(sql3)
    Thread.sleep(2000)

    //read
    reader.read()

    //sleep and check queue size
    while (queue.size() < 2) {
      Thread.sleep(5000)
    }

    queue.size() shouldBe 2

    val sourceRecords2 = QueueHelpers.drainQueue(queue, 100).asScala.toList
    sourceRecords2.size shouldBe 2

    //don't insert any new rows
    reader.read()

    //sleep and check queue size
    while (reader.isQuerying) {
      Thread.sleep(5000)
    }

    //should be empty
    queue.size() shouldBe 0

    //insert another two records
    val sql4 = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 4, 5, 'magic_string4', now());"
    session.execute(sql4)

    Thread.sleep(2000)
    val sql5 = s"INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2" +
      "(id, int_field, long_field, string_field, timestamp_field) " +
      "VALUES ('id4', 5, 6, 'magic_string5', now());"
    session.execute(sql5)

    //read!!!!
    reader.read()

    //sleep and check queue size
    while (queue.size() < 2) {
      Thread.sleep(5000)
    }

    //should be 2!!!
    queue.size() shouldBe 2

  }

  "CassandraReader should read double columns incremental mode" in {
    val session =  createTableAndKeySpace(CASSANDRA_SOURCE_KEYSPACE, secure = true, ssl = false)


    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(getCassandraConfigSourcePropsDoubleIncr)

    //queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(
      session,
      setting,
      taskContext,
      queue)
    reader.read()

    queue.size() shouldBe 0

    //insert another two records
    val sql =
      s"""
         |INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE4
         |(id, int_field, double_field,timestamp_field)
         |VALUES ('id1', 4, 111.1, now());""".stripMargin
    session.execute(sql)
    Thread.sleep(2000)

    //read
    reader.read()

    //sleep and check queue size
    while (queue.size() < 1) {
      Thread.sleep(5000)
    }

    queue.size() shouldBe 1

    val sourceRecords = QueueHelpers.drainQueue(queue, 100).asScala.toList
    sourceRecords.size shouldBe 1

    val struct = sourceRecords.head.value().asInstanceOf[Struct]
    //check a field
    struct.get("double_field") shouldBe 111.1
  }
}


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
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.data.{Schema, Struct}
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{DoNotDiscover, Matchers, WordSpec}

import scala.collection.JavaConverters._
import com.datastax.driver.core.Session

/**
  */
class TestCassandraSourceTaskTimeuuidLong extends WordSpec 
  with Matchers 
  with MockitoSugar 
  with TestConfig
  with ConverterUtil {

  "CassandraReader should read in incremental mode with timeuuid and time slices (long)" in {
    val session =  createTableAndKeySpace(CASSANDRA_SOURCE_KEYSPACE ,secure = true, ssl = false)

    val taskContext = getSourceTaskContextDefault
    val taskConfig  = new CassandraConfigSource(getCassandraConfigSourcePropsTimeuuidIncr)

    // queue for reader to put records in
    val queue = new LinkedBlockingQueue[SourceRecord](100)
    val setting = CassandraSettings.configureSource(taskConfig).head
    val reader = CassandraTableReader(session = session, setting = setting, context = taskContext, queue = queue)
   
    insertIntoTableTwo(session, "id1", "magic_string")

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
    insertIntoTableTwo(session, "id2", "magic_string2")
    insertIntoTableTwo(session, "id3", "magic_string3")

    // sleep for longer than time slice (10 sec)
    Thread.sleep(11000)
    
    // insert another record
    insertIntoTableTwo(session, "id4", "magic_string4")
    
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

  }

  private def insertIntoTableTwo(session: Session, anId: String, stringValue: String) {
    val sql = s"""INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE2
      (id, int_field, long_field, string_field, timeuuid_field)
      VALUES
      ('$anId', 2, 3, '$stringValue', now());"""

    // insert
    session.execute(sql)

    // wait for Cassandra write
    Thread.sleep(1000)
  }  
  
}


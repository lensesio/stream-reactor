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
import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }

import scala.collection.JavaConverters._
import org.apache.kafka.connect.data.Schema
import com.datastax.driver.core.Session
import org.apache.kafka.connect.source.SourceRecord

/**
 * test incremental mode specifying a subset of table columns
 * note: the timestamp column is required as part of SELECT
 */
class TestCassandraSourceTaskTokenCql extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar with TestConfig
    with ConverterUtil {

  before {
    startEmbeddedCassandra()
  }

  after {
    stopEmbeddedCassandra()
  }

  "A Cassandra SourceTask should read records with only columns specified using Token CQL" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    // insert 
    insert(session, "chewie")
    insert(session, "yoda")

    // the incrementalmode of token should override the timestamptype config 
    val taskContext = getSourceTaskContextDefault
    val config = {
      Map(
        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
        CassandraConfigConstants.USERNAME -> USERNAME,
        CassandraConfigConstants.PASSWD -> PASSWD,
        CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
        CassandraConfigConstants.SOURCE_KCQL_QUERY -> s"INSERT INTO sink_test SELECT id, string_field FROM $TABLE5 PK id BATCH=1 INCREMENTALMODE=TOKEN",
        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE5",
        CassandraConfigConstants.POLL_INTERVAL -> "1000",
        CassandraConfigConstants.FETCH_SIZE -> "10",
        CassandraConfigConstants.TIMESTAMP_TYPE -> "timestamp").asJava
    }

    //get task
    val task = new CassandraSourceTask()
    task.initialize(taskContext)
    task.start(config)

    val records = pollAndWait(task)

    records.size() shouldBe 1
    
    val sourceRecord = records.asScala.head
    val json: JsonNode = convertValueToJson(sourceRecord)
    
    val recordId = json.get("id").asText()
    recordId.size > 0
    
    val stringField = json.get("string_field").asText()
    stringField.equals("yoda") || stringField.equals("chewie") shouldBe true
    json.get("timestamp_field") shouldBe null
    json.get("int_field") shouldBe null
    
    //
    // call 2nd time
    //
    val nextRecords = pollAndWait(task)
    
    nextRecords.size() shouldBe 1
    
    val nextSourceRecord = nextRecords.asScala.head
    val nextJson: JsonNode = convertValueToJson(nextSourceRecord)
    
    val nextRecordId = nextJson.get("id").asText()
    nextRecordId.size > 0
    nextRecordId.equals(recordId) shouldBe false
    
    val nextStringField = nextJson.get("string_field").asText()
    nextStringField.equals("yoda") || nextStringField.equals("chewie") shouldBe true
    nextStringField.equals(recordId) shouldBe false
    nextJson.get("timestamp_field") shouldBe null
    nextJson.get("int_field") shouldBe null
    
    //stop task
    task.stop()
  }

  private def pollAndWait(task: CassandraSourceTask): java.util.List[SourceRecord] = {
    // trigger poll to have the readers execute a query and add to the queue
    task.poll()

    // wait a little for the poll to catch the records
    while (task.queueSize(TABLE5) == 0) {
      Thread.sleep(5000)
    }

    // call poll again to drain the queue
    val records = task.poll()
    records
  }
  
  private def insert(session: Session, myValue: String) {
    val sql = s"""INSERT INTO $CASSANDRA_KEYSPACE.$TABLE5
      (id, int_field, long_field, string_field, another_time_field) 
      VALUES 
      (now(), 2, 3, '$myValue', now());"""

    // insert
    session.execute(sql)

    // wait for Cassandra write
    Thread.sleep(1000)
  }
}



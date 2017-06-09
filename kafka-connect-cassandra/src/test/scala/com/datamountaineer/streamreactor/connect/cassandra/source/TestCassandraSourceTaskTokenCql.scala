///*
// * Copyright 2017 Datamountaineer.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.datamountaineer.streamreactor.connect.cassandra.source
//
//import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
//import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
//import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
//import com.fasterxml.jackson.databind.JsonNode
//import org.scalatest.mock.MockitoSugar
//import org.scalatest.{ BeforeAndAfter, Matchers, WordSpec }
//
//import scala.collection.JavaConverters._
//import com.datastax.driver.core.Session
//import org.apache.kafka.connect.source.SourceRecord
//
///**
// * test incremental mode specifying a subset of table columns
// * note: the timestamp column is required as part of SELECT
// */
//class TestCassandraSourceTaskTokenCql extends WordSpec with Matchers with BeforeAndAfter with MockitoSugar with TestConfig
//    with ConverterUtil {
//
//  before {
//    startEmbeddedCassandra("cassandra.ByteOrderedPartitioner.yaml")
//  }
//
//  after {
//    stopEmbeddedCassandra()
//  }
//
//  "A Cassandra SourceTask should read records with only columns specified using Token CQL" in {
//    val session = createTableAndKeySpace(CASSANDRA_SOURCE_KEYSPACE , secure = true, ssl = false)
//
//    // insert
//    insert(session, "chewie")
//    insert(session, "yoda")
//
//    val taskContext = getSourceTaskContextDefault
//    val config = {
//      Map(
//        CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
//        CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SOURCE_KEYSPACE,
//        CassandraConfigConstants.USERNAME -> USERNAME,
//        CassandraConfigConstants.PASSWD -> PASSWD,
//        CassandraConfigConstants.SOURCE_KCQL_QUERY -> s"INSERT INTO sink_test SELECT id, string_field FROM $TABLE5 PK id BATCH=2 INCREMENTALMODE=TOKEN",
//        CassandraConfigConstants.ASSIGNED_TABLES -> s"$TABLE5",
//        CassandraConfigConstants.POLL_INTERVAL -> "1000",
//        CassandraConfigConstants.FETCH_SIZE -> "10").asJava
//    }
//
//    //get task
//    val task = new CassandraSourceTask()
//    task.initialize(taskContext)
//    task.start(config)
//
//    var records = pollAndWait(task)
//
//    records.size() shouldBe 2
//
//    var sourceRecord = records.asScala(0)
//    checkSourceRecord(sourceRecord, "chewie")
//
//    sourceRecord = records.asScala(1)
//    checkSourceRecord(sourceRecord, "yoda")
//
//    //
//    // insert more data
//    //
//    insert(session, "han solo")
//    insert(session, "greedo")
//    insert(session, "kenobi")
//    insert(session, "windu")
//
//    //
//    // call 2nd time
//    //
//    records = pollAndWait(task)
//
//    records.size() shouldBe 2
//
//    sourceRecord = records.asScala(0)
//    checkSourceRecord(sourceRecord, "han solo")
//
//    sourceRecord = records.asScala(1)
//    checkSourceRecord(sourceRecord, "greedo")
//
//    //stop task
//    task.stop()
//  }
//
//  private def checkSourceRecord(sourceRecord: SourceRecord, expectedString: String) = {
//    val json: JsonNode = convertValueToJson(sourceRecord)
//
//    val recordId = json.get("id").asText()
//    recordId.size > 0
//
//    val stringField = json.get("string_field").asText()
//    stringField shouldBe expectedString
//    json.get("timestamp_field") shouldBe null
//    json.get("int_field") shouldBe null
//  }
//
//  private def pollAndWait(task: CassandraSourceTask): java.util.List[SourceRecord] = {
//    // trigger poll to have the readers execute a query and add to the queue
//    task.poll()
//
//    // wait a little for the poll to catch the records
//    while (task.queueSize(TABLE5) == 0) {
//      Thread.sleep(5000)
//    }
//
//    // call poll again to drain the queue
//    val records = task.poll()
//    records
//  }
//
//  private def insert(session: Session, myValue: String) {
//    val sql = s"""INSERT INTO $CASSANDRA_SOURCE_KEYSPACE.$TABLE5
//      (id, int_field, long_field, string_field, another_time_field)
//      VALUES
//      (now(), 2, 3, '$myValue', now());"""
//
//    // insert
//    session.execute(sql)
//
//    // wait for Cassandra write
//    Thread.sleep(1000)
//  }
//}
//
//

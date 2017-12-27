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


import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.common.config.ConfigException
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{DoNotDiscover, Matchers, WordSpec}

import scala.collection.JavaConverters._
import com.datastax.driver.core.Session
import org.apache.kafka.connect.data.Schema
import org.scalatest.BeforeAndAfterAll

@DoNotDiscover
class TestCassandraSourceTaskTimestamp extends WordSpec
    with Matchers
    with MockitoSugar
    with TestConfig
    with TestCassandraSourceUtil
    with ConverterUtil 
    with BeforeAndAfterAll {

  var session: Session = _
  val keyspace = "source"
  var tableName: String = _

  override def beforeAll {
    session = createKeySpace(keyspace, secure = true, ssl = false)
    tableName = createTimestampTable(session, keyspace)
  }

  override def afterAll(): Unit = {
    session.close()
    session.getCluster.close()
  }

  "A Cassandra SourceTask should read in incremental mode with timestamp and time slices" in {
    val taskContext = getSourceTaskContextDefault
    val config = getCassandraConfigDefault
    val task = new CassandraSourceTask()
    task.initialize(taskContext)

    //start task
    task.start(config)

    insertIntoTimestampTable(session, keyspace, tableName, "id1", "magic_string", getFormattedDateNow)
    
    var records = pollAndWait(task, tableName)
    var sourceRecord = records.asScala.head
    // check JSON fields
    var json: JsonNode = convertValueToJson(sourceRecord)
    println(json)
    json.get("string_field").asText().equals("magic_string") shouldBe true
    json.get("timestamp_field").asText().size > 0
    json.get("int_field") shouldBe null

    insertIntoTimestampTable(session, keyspace, tableName, "id2", "magic_string2", getFormattedDateNow)

    records = pollAndWait(task, tableName)
    sourceRecord = records.asScala.head
    // check JSON fields
    json = convertValueToJson(sourceRecord)
    println(json)
    json.get("string_field").asText().equals("magic_string2") shouldBe true
    json.get("timestamp_field").asText().size > 0
    json.get("int_field") shouldBe null

    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should read in incremental mode with timestamp and time slices and use ignore and unwrap" in {

    val taskContext = getSourceTaskContextDefault
    val config = getCassandraConfigWithUnwrap
    val task = new CassandraSourceTask()
    task.initialize(taskContext)

    //start task
    task.start(config)

    insertIntoTimestampTable(session, keyspace, tableName, "id1", "magic_string", getFormattedDateNow)

    val records = pollAndWait(task, tableName)
    val sourceRecord = records.asScala.head
    sourceRecord.keySchema shouldBe null
    sourceRecord.key shouldBe null
    sourceRecord.valueSchema shouldBe Schema.STRING_SCHEMA
    sourceRecord.value shouldBe "magic_string"

    //stop task
    task.stop()
  }

  "A Cassandra SourceTask should throw exception when timestamp column is not specified" in {

    val taskContext = getSourceTaskContextDefault
    val config = getCassandraConfigWithKcqlNoPrimaryKeyInSelect
    val task = new CassandraSourceTask()
    task.initialize(taskContext)

    insertIntoTimestampTable(session, keyspace, tableName, "id1", "magic_string", getFormattedDateNow)

    try {
      task.start(config)
      fail()
    } catch {
      case _: ConfigException => // Expected, so continue
    }
    task.stop()
  }

  private def getCassandraConfigWithKcqlNoPrimaryKeyInSelect() = {
    val myKcql = s"INSERT INTO sink_test SELECT string_field FROM $tableName PK timestamp_field INCREMENTALMODE=timestamp"
    getCassandraConfig(keyspace, tableName, myKcql)
  }

  private def getCassandraConfigWithUnwrap() = {
    val myKcql = s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $tableName IGNORE timestamp_field PK timestamp_field WITHUNWRAP INCREMENTALMODE=timestamp"
    getCassandraConfig(keyspace, tableName, myKcql)
  }

  private def getCassandraConfigDefault() = {
    val myKcql = s"INSERT INTO sink_test SELECT string_field, timestamp_field FROM $tableName PK timestamp_field INCREMENTALMODE=timestamp"
    getCassandraConfig(keyspace, tableName, myKcql)
  }

}


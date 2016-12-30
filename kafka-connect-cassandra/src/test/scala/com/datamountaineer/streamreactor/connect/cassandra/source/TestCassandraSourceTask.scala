/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.cassandra.source


import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.fasterxml.jackson.databind.JsonNode
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

  "A Cassandra SourceTask should start and read records from Cassandra" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)

    val sql = s"INSERT INTO $CASSANDRA_KEYSPACE.$TABLE2" +
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
      Thread.sleep(1000)
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
}


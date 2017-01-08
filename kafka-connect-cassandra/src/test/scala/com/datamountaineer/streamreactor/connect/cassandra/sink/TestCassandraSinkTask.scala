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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.util

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import com.datastax.driver.core.ConsistencyLevel
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConverters._

class TestCassandraSinkTask extends WordSpec with Matchers with MockitoSugar with BeforeAndAfterAll with TestConfig {

  override def beforeAll(): Unit = {
    startEmbeddedCassandra()
  }

  "A Cassandra SinkTask" should {
    "start and write records to Cassandra" in {
      val session = createTableAndKeySpace(secure = true)
      //mock the context to return our assignment when called
      val context = mock[SinkTaskContext]
      val assignment = getAssignment
      when(context.assignment()).thenReturn(assignment)
      //get test records
      val testRecords = getTestRecords(TABLE1)
      //get config
      val config = getCassandraConfigSinkProps
      //get task
      val task = new CassandraSinkTask()
      //initialise the tasks context
      task.initialize(context)
      //start task
      task.start(config)
      //simulate the call from Connect
      task.put(testRecords.asJava)
      //stop task
      task.stop()

      //check we can get back what we wrote
      val res = session.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC1")
      res.all().size() shouldBe testRecords.size
    }

    "start and write records to Cassandra using ONE as consistency level" in {
      val session = createTableAndKeySpace(secure = true)
      //mock the context to return our assignment when called
      val context = mock[SinkTaskContext]
      val assignment = getAssignment
      when(context.assignment()).thenReturn(assignment)
      //get test records
      val testRecords = getTestRecords(TABLE1)
      //get config
      val config = new util.HashMap[String, String](getCassandraConfigSinkProps)
      config.put(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG, ConsistencyLevel.ONE.toString)
      //get task
      val task = new CassandraSinkTask()
      //initialise the tasks context
      task.initialize(context)
      //start task
      task.start(config)
      //simulate the call from Connect
      task.put(testRecords.asJava)
      //stop task
      task.stop()

      //check we can get back what we wrote
      val res = session.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TOPIC1")
      res.all().size() shouldBe testRecords.size
    }
  }
}

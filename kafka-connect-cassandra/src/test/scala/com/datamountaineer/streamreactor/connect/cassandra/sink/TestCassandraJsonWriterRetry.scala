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

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigSink
import org.apache.kafka.connect.errors.RetriableException
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

/**
  * Created by andrew@datamountaineer.com on 04/05/16. 
  * stream-reactor
  */
class TestCassandraJsonWriterRetry extends WordSpec with Matchers with MockitoSugar with BeforeAndAfter with TestConfig {
  before {
    startEmbeddedCassandra()
  }

  after{
    stopEmbeddedCassandra()
  }

  "Cassandra JsonWriter with Retry should throw Retriable Exception" in {
    val session = createTableAndKeySpace(secure = true, ssl = false)
    val context = mock[SinkTaskContext]
    val assignment = getAssignment
    when(context.assignment()).thenReturn(assignment)
    //get test records
    val testRecords = getTestRecords(TABLE1)
    //get config
    val props  = getCassandraConfigSinkPropsRetry
    val taskConfig = new CassandraConfigSink(props)
    val writer = CassandraWriter(taskConfig, context)


    //drop table in cassandra
    session.execute(s"DROP TABLE IF EXISTS $CASSANDRA_KEYSPACE.$TABLE1")
    intercept[RetriableException] {
      writer.write(testRecords)
    }

    session.close()

    //put back table
    val session2 = createTableAndKeySpace(secure = true, ssl = false)
    writer.write(testRecords)
    Thread.sleep(2000)
    //check we can get back what we wrote
    val res = session2.execute(s"SELECT * FROM $CASSANDRA_KEYSPACE.$TABLE1")
    res.all().size() shouldBe testRecords.size
  }
}

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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraConfigConstants
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava}

class TestCassandraSinkConnector extends AnyWordSpec with BeforeAndAfter with Matchers with TestConfig {
  "Should start a Cassandra Sink Connector" in {
    val props =  Map(
      "topics" -> s"$TOPIC1, $TOPIC2",
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.KCQL -> QUERY_ALL
    ).asJava

    val connector = new CassandraSinkConnector()
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.asScala.head.get(CassandraConfigConstants.KCQL) shouldBe QUERY_ALL
    taskConfigs.asScala.head.get(CassandraConfigConstants.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfigs.asScala.head.get(CassandraConfigConstants.KEY_SPACE) shouldBe TOPIC1
    taskConfigs.size() shouldBe 1
    connector.taskClass() shouldBe classOf[CassandraSinkTask]
    //connector.version() shouldBe ""
    connector.stop()
  }
}

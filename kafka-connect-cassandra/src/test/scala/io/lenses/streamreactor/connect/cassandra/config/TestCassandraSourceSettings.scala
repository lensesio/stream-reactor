/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cassandra.config

import io.lenses.streamreactor.connect.cassandra.TestConfig
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 28/04/16.
  * stream-reactor
  */
class TestCassandraSourceSettings extends AnyWordSpec with Matchers with TestConfig {
  "CassandraSettings should return setting for a source" in {
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS  -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE       -> CASSANDRA_SOURCE_KEYSPACE,
      CassandraConfigConstants.USERNAME        -> USERNAME,
      CassandraConfigConstants.PASSWD          -> PASSWD,
      CassandraConfigConstants.KCQL            -> IMPORT_QUERY_ALL,
      CassandraConfigConstants.ASSIGNED_TABLES -> ASSIGNED_TABLES,
      CassandraConfigConstants.POLL_INTERVAL   -> "1000",
    ).asJava

    val taskConfig = CassandraConfigSource(props)
    val settings   = CassandraSettings.configureSource(taskConfig).toList
    settings.size shouldBe 2
    settings.head.kcql.getSource shouldBe TABLE1
    settings.head.kcql.getTarget shouldBe TABLE1 //no table mapping provided so should be the table
    settings.head.timestampColType shouldBe TimestampType.NONE
    settings(1).kcql.getSource shouldBe TABLE2
    settings(1).kcql.getTarget shouldBe TOPIC2
    settings(1).timestampColType shouldBe TimestampType.NONE
  }

  "CassandraSettings should return setting for a source with one table" in {
    val map = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE      -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME       -> USERNAME,
      CassandraConfigConstants.PASSWD         -> PASSWD,
      CassandraConfigConstants.KCQL           -> "INSERT INTO cassandra-source SELECT * FROM orders PK created",
      CassandraConfigConstants.POLL_INTERVAL  -> "1000",
    )
    val taskConfig = CassandraConfigSource(map.asJava)
    val settings   = CassandraSettings.configureSource(taskConfig).toList
    settings.size shouldBe 1
  }
}

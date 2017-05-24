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

package com.datamountaineer.streamreactor.connect.cassandra.config

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 28/04/16. 
  * stream-reactor
  */
class TestCassandraSourceSettings extends WordSpec with Matchers with TestConfig {
  "CassandraSettings should return setting for a source" in {
    val taskConfig = CassandraConfigSource(getCassandraConfigSourcePropsBulk)
    val assigned = List(TABLE1, TABLE2)
    val settings = CassandraSettings.configureSource(taskConfig).toList
    settings.size shouldBe 2
    settings.head.routes.getSource shouldBe TABLE1
    settings.head.routes.getTarget shouldBe TABLE1 //no table mapping provided so should be the table
    settings.head.bulkImportMode shouldBe true
    settings(1).routes.getSource shouldBe TABLE2
    settings(1).routes.getTarget shouldBe TOPIC2
    settings(1).bulkImportMode shouldBe true
  }

  "CassandraSettings should return setting for a source with one table" in {
    val map = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.ROUTE_QUERY -> "INSERT INTO cassandra-source SELECT * FROM orders PK created",
      CassandraConfigConstants.IMPORT_MODE -> CassandraConfigConstants.INCREMENTAL,
      CassandraConfigConstants.POLL_INTERVAL -> "1000"
    )
    val taskConfig = CassandraConfigSource(map)
    val settings = CassandraSettings.configureSource(taskConfig).toList
    settings.size shouldBe 1
  }
}

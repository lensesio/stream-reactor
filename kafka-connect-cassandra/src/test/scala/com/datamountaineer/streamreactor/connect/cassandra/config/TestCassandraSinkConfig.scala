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
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class TestCassandraSinkConfig extends AnyWordSpec with BeforeAndAfter with Matchers with TestConfig {

  "A CassandraConfig should return configured for username and password" in {
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.KCQL -> QUERY_ALL
    ).asJava

    val taskConfig = CassandraConfigSink(props)
    taskConfig.getString(CassandraConfigConstants.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfig.getString(CassandraConfigConstants.KEY_SPACE) shouldBe CASSANDRA_SINK_KEYSPACE
    taskConfig.getString(CassandraConfigConstants.USERNAME) shouldBe USERNAME
    taskConfig.getPassword(CassandraConfigConstants.PASSWD).value shouldBe PASSWD
    taskConfig.getString(CassandraConfigConstants.KCQL) shouldBe QUERY_ALL
    taskConfig.getString(CassandraConfigConstants.CONSISTENCY_LEVEL_CONFIG) shouldBe CassandraConfigConstants.CONSISTENCY_LEVEL_DEFAULT
  }

  "A CassandraConfig should return configured for SSL" in {
    val props = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.SSL_ENABLED -> "true",
      CassandraConfigConstants.TRUST_STORE_PATH -> TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD -> TRUST_STORE_PASSWORD,
      CassandraConfigConstants.KCQL -> QUERY_ALL
    ).asJava

    val taskConfig  = CassandraConfigSink(props)
    taskConfig.getString(CassandraConfigConstants.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfig.getString(CassandraConfigConstants.KEY_SPACE) shouldBe CASSANDRA_SINK_KEYSPACE
    taskConfig.getString(CassandraConfigConstants.USERNAME) shouldBe USERNAME
    taskConfig.getPassword(CassandraConfigConstants.PASSWD).value shouldBe PASSWD
    taskConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED) shouldBe true
    taskConfig.getString(CassandraConfigConstants.TRUST_STORE_PATH) shouldBe TRUST_STORE_PATH
    taskConfig.getPassword(CassandraConfigConstants.TRUST_STORE_PASSWD).value shouldBe TRUST_STORE_PASSWORD
    //taskConfig.getString(CassandraConfigConstants.EXPORT_MAPPINGS) shouldBe EXPORT_TOPIC_TABLE_MAP
    taskConfig.getString(CassandraConfigConstants.KCQL) shouldBe QUERY_ALL
  }

  "A CassandraConfig should return configured for SSL without client certficate authentication" in {
    val props =     Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.SSL_ENABLED -> "true",
      CassandraConfigConstants.TRUST_STORE_PATH -> TRUST_STORE_PATH,
      CassandraConfigConstants.TRUST_STORE_PASSWD -> TRUST_STORE_PASSWORD,
      CassandraConfigConstants.USE_CLIENT_AUTH -> "false",
      CassandraConfigConstants.KEY_STORE_PATH -> KEYSTORE_PATH,
      CassandraConfigConstants.KEY_STORE_PASSWD -> KEYSTORE_PASSWORD,
      CassandraConfigConstants.KCQL -> QUERY_ALL
    ).asJava

    val taskConfig  = CassandraConfigSink(props)
    taskConfig.getString(CassandraConfigConstants.CONTACT_POINTS) shouldBe CONTACT_POINT
    taskConfig.getString(CassandraConfigConstants.KEY_SPACE) shouldBe CASSANDRA_SINK_KEYSPACE
    taskConfig.getString(CassandraConfigConstants.USERNAME) shouldBe USERNAME
    taskConfig.getPassword(CassandraConfigConstants.PASSWD).value shouldBe PASSWD
    taskConfig.getBoolean(CassandraConfigConstants.SSL_ENABLED) shouldBe true
    taskConfig.getString(CassandraConfigConstants.KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value shouldBe KEYSTORE_PASSWORD
    taskConfig.getBoolean(CassandraConfigConstants.USE_CLIENT_AUTH) shouldBe false
    taskConfig.getString(CassandraConfigConstants.KEY_STORE_PATH) shouldBe KEYSTORE_PATH
    taskConfig.getPassword(CassandraConfigConstants.KEY_STORE_PASSWD).value shouldBe KEYSTORE_PASSWORD
    taskConfig.getString(CassandraConfigConstants.KCQL) shouldBe QUERY_ALL
  }
}

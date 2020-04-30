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

package com.datamountaineer.streamreactor.connect.voltdb.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class VoltSettingsTest extends AnyWordSpec with Matchers {

  val MEASURE_NAME = "someMeasurement"
  val TOPIC_NAME = "mykafkatopic"
  val QUERY_ALL = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME"
  val QUERY_SELECT = s"INSERT INTO $MEASURE_NAME SELECT lastName as surname, firstName FROM $TOPIC_NAME"
  val QUERY_SELECT_AND_TIMESTAMP = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME WITHTIMESTAMP ts"
  val QUERY_SELECT_AND_TIMESTAMP_SYSTEM = s"INSERT INTO $MEASURE_NAME SELECT * FROM $TOPIC_NAME WITHTIMESTAMP ${Kcql.TIMESTAMP}"

  "raise a configuration exception if the connection servers is missing" in {
    val props = Map(
      VoltSinkConfigConstants.KCQL_CONFIG->QUERY_ALL
    ).asJava


    intercept[ConfigException] {
      VoltSinkConfig(props)
    }
  }

  //  "raise a configuration exception if the user is not set" in {
  //    intercept[ConfigException] {
  //      val config = mock[VoltSinkConfig]
  //      when(config.getString(VoltSinkConfigConstants.SERVERS_CONFIG)).thenReturn("localhost:8081")
  //      //when(config.getString(VoltSinkConfigConstants.USER_CONFIG)).thenReturn("")
  //      //when(config.getString(VoltSinkConfigConstants.PASSWORD_CONFIG)).thenReturn("apass")
  //      when(config.getString(VoltSinkConfigConstants.ERROR_POLICY_CONFIG)).thenReturn("THROW")
  //      when(config.getString(VoltSinkConfigConstants.EXPORT_ROUTE_QUERY_CONFIG)).thenReturn(QUERY_ALL)
  //      VoltSettings(config)
  //    }
  //  }

  "create a settings with all fields" in {
    val servers = "localhost:8081"
    val user = "myuser"
    val pass = "mememe"
    val props = Map(
      VoltSinkConfigConstants.USER_CONFIG->user,
      VoltSinkConfigConstants.KCQL_CONFIG->QUERY_ALL,
      VoltSinkConfigConstants.SERVERS_CONFIG->servers,
      VoltSinkConfigConstants.PASSWORD_CONFIG->pass
    ).asJava

    val config = VoltSinkConfig(props)
    val settings = VoltSettings(config)
    settings.servers shouldBe servers
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.fieldsExtractorMap.size shouldBe 1
    settings.fieldsExtractorMap(TOPIC_NAME).includeAllFields shouldBe true
    settings.fieldsExtractorMap(TOPIC_NAME).fieldsAliasMap shouldBe Map("*"->"*")
  }

  "create a settings with selected fields" in {
    val servers = "localhost:8081"
    val user = "myuser"
    val pass = "mememe"
    val props = Map(
      VoltSinkConfigConstants.KCQL_CONFIG->QUERY_SELECT,
      VoltSinkConfigConstants.SERVERS_CONFIG->servers,
      VoltSinkConfigConstants.USER_CONFIG->user,
      VoltSinkConfigConstants.PASSWORD_CONFIG->pass
    ).asJava

    val config = VoltSinkConfig(props)
    val settings = VoltSettings(config)
    settings.servers shouldBe servers
    //settings.user shouldBe user
    //settings.password shouldBe "mememe"
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.fieldsExtractorMap.size shouldBe 1
    settings.fieldsExtractorMap(TOPIC_NAME).includeAllFields shouldBe false
    settings.fieldsExtractorMap(TOPIC_NAME).fieldsAliasMap shouldBe Map("firstName" -> "firstName", "lastName" -> "surname")
  }

  "create a settings with selected fields with timestamp set to a field" in {
    val servers = "localhost:8081"
    val user = "myuser"
    val pass = "mememe"
    val props = Map(
      VoltSinkConfigConstants.KCQL_CONFIG->QUERY_SELECT_AND_TIMESTAMP,
      VoltSinkConfigConstants.SERVERS_CONFIG->servers,
      VoltSinkConfigConstants.USER_CONFIG->user,
      VoltSinkConfigConstants.PASSWORD_CONFIG->pass
    ).asJava

    val config = VoltSinkConfig(props)
    val settings = VoltSettings(config)
    settings.servers shouldBe servers
    // settings.user shouldBe user
    // settings.password shouldBe "mememe"
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.fieldsExtractorMap.size shouldBe 1
    settings.fieldsExtractorMap(TOPIC_NAME).includeAllFields shouldBe true
    settings.fieldsExtractorMap(TOPIC_NAME).fieldsAliasMap shouldBe Map("*"->"*")
  }

  "create a settings with selected fields with timestamp set to a sys_timestamp" in {
    val servers = "localhost:8081"
    val user = "myuser"
    val pass = "mememe"
    val props = Map(
      VoltSinkConfigConstants.KCQL_CONFIG->QUERY_SELECT_AND_TIMESTAMP_SYSTEM,
      VoltSinkConfigConstants.SERVERS_CONFIG->servers,
      VoltSinkConfigConstants.USER_CONFIG->user,
      VoltSinkConfigConstants.PASSWORD_CONFIG->pass
    ).asJava

    val config = VoltSinkConfig(props)
    val settings = VoltSettings(config)
    settings.servers shouldBe servers
    // settings.user shouldBe user
    //settings.password shouldBe "mememe"
    settings.errorPolicy shouldBe ThrowErrorPolicy()
    settings.fieldsExtractorMap.size shouldBe 1
    settings.fieldsExtractorMap(TOPIC_NAME).includeAllFields shouldBe true
    settings.fieldsExtractorMap(TOPIC_NAME).fieldsAliasMap shouldBe Map("*"->"*")
  }
}

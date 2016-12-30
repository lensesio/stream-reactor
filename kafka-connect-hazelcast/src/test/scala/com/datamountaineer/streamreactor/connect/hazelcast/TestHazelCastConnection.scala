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

package com.datamountaineer.streamreactor.connect.hazelcast

import com.datamountaineer.streamreactor.connect.hazelcast.config.{HazelCastSinkConfig, HazelCastSinkSettings}
import com.hazelcast.config.Config
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 10/08/16. 
  * stream-reactor
  */
class TestHazelCastConnection extends TestBase {
  "should connect to a Hazelcast cluster" in {
    val configApp1 = new Config()
    configApp1.getGroupConfig.setName(GROUP_NAME).setPassword(HazelCastSinkConfig.SINK_GROUP_PASSWORD_DEFAULT)
    val instance = Hazelcast.newHazelcastInstance(configApp1)
    val props = getProps
    val config = new HazelCastSinkConfig(props)
    val settings = HazelCastSinkSettings(config)
    val conn = HazelCastConnection(settings.connConfig)
    conn.isInstanceOf[HazelcastInstance] shouldBe true
    val connectedClients = instance.getClientService.getConnectedClients.toSet
    connectedClients.size shouldBe 1
    connectedClients.head.getSocketAddress.getHostName shouldBe "localhost"
    conn.shutdown()
    instance.shutdown()
  }
}

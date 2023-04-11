/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.mqtt.sink

import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.jdk.CollectionConverters.MapHasAsJava

class MqttSinkConnectorTest extends AnyWordSpecLike with Matchers {

  private val testClientID = "KafkaRocks"
  private val exampleTopic = "demoTopic"
  private val exampleKcql  = "INSERT INTO mqttSource SELECT * FROM " + exampleTopic

  private val baseProps = Map(
    "topics"    -> exampleTopic,
    KCQL_CONFIG -> exampleKcql,
  )

  "MqttSinkConnector" should {

    "use specified client ID for single task" in {
      val props       = baseProps + (CLIENT_ID_CONFIG -> testClientID)
      val taskConfigs = createTaskConfigs(props, 1)
      taskConfigs should have size 1
      taskConfigs.get(0).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be(testClientID)
    }

    "omit client ID for single task" in {
      val taskConfigs = createTaskConfigs(baseProps, 1)
      taskConfigs should have size 1
      taskConfigs.get(0).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be(null)
    }

    "create unique client IDs for task configs when client ID supplied" in {
      val props       = baseProps + (CLIENT_ID_CONFIG -> testClientID)
      val taskConfigs = createTaskConfigs(props, 4)
      taskConfigs should have size 4
      taskConfigs.get(0).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be("KafkaRocks-1")
      taskConfigs.get(1).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be("KafkaRocks-2")
      taskConfigs.get(2).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be("KafkaRocks-3")
      taskConfigs.get(3).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be("KafkaRocks-4")
    }

    /**
      * If a client ID is not set for multiple tasks it will be set to a random value in the MqttSinkSettings.apply()
      */
    "omit client ID when not set for multiple tasks" in {
      val props       = baseProps
      val taskConfigs = createTaskConfigs(props, 6)
      taskConfigs should have size 6
      (0 to 5).map(idx => taskConfigs.get(idx).get(MqttConfigConstants.CLIENT_ID_CONFIG) should be(null))
    }
  }

  private def createTaskConfigs(props: Map[String, String], numTasks: Int) = {
    val mqttSinkConnector = new MqttSinkConnector()
    mqttSinkConnector.start(props.asJava)
    mqttSinkConnector.taskConfigs(numTasks)
  }
}

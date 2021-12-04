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

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.util
import com.datamountaineer.streamreactor.connect.mqtt.config.{MqttConfigConstants, MqttSourceConfig, MqttSourceSettings}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava}


class MqttSourceConnectorTest extends AnyWordSpec with Matchers {
  val baseProps: Map[String, String] = Map(
    MqttConfigConstants.HOSTS_CONFIG -> "tcp://0.0.0.0:1883",
    MqttConfigConstants.QS_CONFIG -> "1"
  )

  val mqttSourceConnector = new MqttSourceConnector()
  val targets = Array("test", "topic", "stream")

  val normalSources = Array(
    "/test/#",
    "/mqttTopic/+/test",
    "/stream",
    "/some/other/topic",
    "/alot/+/+/fourth"
  )
  val sharedSources = Array(
    "$share/some-group/test/#",
    "$share/aservice/mqttTopic/+/test",
    "$share/connectorGroup/stream",
    "$share/g1/some/other/topic",
    "$share/grouped/alot/+/+/fourth"
  )

  val normalKcql: Array[String] = normalSources.zip(targets).map{
    case (source, target) =>  s"INSERT INTO `$target` SELECT * FROM `$source`"
  }
  val sharedKcql: Array[String] = sharedSources.zip(targets).map{
    case (source, target) =>  s"INSERT INTO `$target` SELECT * FROM `$source`"
  }
  val allKcql: Array[String] = normalKcql ++ sharedKcql

  "The MqttSourceConnector" should {
    "indicate that shared subscription instructions should be replicated" in {
      all (sharedKcql.map(mqttSourceConnector.shouldReplicate)) should be (true)
    }

    "indicate that normal subscription instructions should not be replicated" in {
      all (normalKcql.map(mqttSourceConnector.shouldReplicate)) should be (false)
    }
  }

  "The MqttSourceConnector" when {
    "the connect.mqtt.share.replicate option is activated" should {
      val replicateProps = baseProps ++ Map(MqttConfigConstants.REPLICATE_SHARED_SUBSCIRPTIONS_CONFIG -> "true")

      "correctly distribute instructions when there is no replicated instruction" in {
        val props = replicateProps ++ Map(MqttConfigConstants.KCQL_CONFIG -> normalKcql.mkString(";"))
        mqttSourceConnector.start(props.asJava)

        val maxTasks = 2
        val kcqls = extractKcqls(mqttSourceConnector.taskConfigs(maxTasks))
        kcqls.flatten should have length normalKcql.length.toLong
      }

      "correctly distribute instructions when there is only replicated instructions" in {
        val props = replicateProps ++ Map(MqttConfigConstants.KCQL_CONFIG -> sharedKcql.mkString(";"))
        mqttSourceConnector.start(props.asJava)

        val maxTasks = 2
        val kcqls = extractKcqls(mqttSourceConnector.taskConfigs(maxTasks))
        all (kcqls) should have length sharedKcql.length.toLong
      }

      "correctly distribute instructions when there is a mix of instructions" in {
        val props = replicateProps ++ Map(MqttConfigConstants.KCQL_CONFIG -> allKcql.mkString(";"))
        mqttSourceConnector.start(props.asJava)

        val maxTasks = 2
        val kcqls = extractKcqls(mqttSourceConnector.taskConfigs(maxTasks))
        kcqls.flatten should have length((sharedKcql.length * maxTasks + normalKcql.length).toLong)
        all (kcqls.map(_.length)) should be >= sharedKcql.length
      }
    }

    "the connect.mqtt.share.replicate option is deactivated" should {
      "not replicate shared instructions" in {
        val props = baseProps ++ Map(MqttConfigConstants.KCQL_CONFIG -> allKcql.mkString(";"))
        mqttSourceConnector.start(props.asJava)

        val maxTasks = 2
        val kcqls = extractKcqls(mqttSourceConnector.taskConfigs(maxTasks))
        kcqls.flatten should have length allKcql.length.toLong
      }
    }
  }

  def extractKcqls(configs: util.List[util.Map[String, String]]): Array[Array[String]] = {
    configs.asScala.map(t => MqttSourceSettings(MqttSourceConfig(t)).kcql).toArray
  }
}

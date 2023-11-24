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
package io.lenses.streamreactor.connect.pulsar.sink

import io.lenses.streamreactor.connect.pulsar.config.PulsarConfigConstants
import org.apache.kafka.common.config.ConfigException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 24/01/2018.
  * stream-reactor
  */
class PulsarSinkConnectorTest extends AnyWordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start a Connector and split correctly" in {
    val props = Map(
      "topics"                           -> "kafka_topic",
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG  -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER = SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000",
    ).asJava

    val connector = new PulsarSinkConnector()
    connector.start(props)
    val taskConfigs = connector.taskConfigs(1)
    taskConfigs.size shouldBe 1
    connector.taskClass() shouldBe classOf[PulsarSinkTask]
    connector.stop()
  }

  "should throw as topic doesn't match kcql" in {
    val props = Map(
      "topics"                           -> "bad",
      PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG  -> s"INSERT INTO $pulsarTopic SELECT * FROM kafka_topic BATCH = 10 WITHPARTITIONER =  SinglePartition WITHCOMPRESSION = ZLIB WITHDELAY = 1000",
    ).asJava

    val connector = new PulsarSinkConnector()

    intercept[ConfigException] {
      connector.start(props)
    }
  }
}

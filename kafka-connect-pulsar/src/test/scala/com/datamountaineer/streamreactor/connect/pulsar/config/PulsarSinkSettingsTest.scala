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
package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.kcql.CompressionType
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 22/01/2018.
  * stream-reactor
  */
class PulsarSinkSettingsTest extends AnyWordSpec with Matchers {

  val topic = "persistent://landoop/standalone/connect/kafka-topic"

  "should produce a valid config" in {
    val config = PulsarSinkConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO $topic SELECT * FROM kafka_topic",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings = PulsarSinkSettings(config)
    settings.kcql.head.getTarget shouldBe topic
  }

  "should have messagemode SinglePartititon" in {
    val config = PulsarSinkConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO $topic SELECT * FROM kafka_topic WITHPARTITIONER = singlepartition",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings = PulsarSinkSettings(config)
    settings.kcql.head.getTarget shouldBe topic
    settings.kcql.head.getWithPartitioner shouldBe "singlepartition"
  }

  "should have messagemode RoundRobinPartition" in {
    val config = PulsarSinkConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO $topic SELECT * FROM kafka_topic WITHPARTITIONER = RoundRobinPartition",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings = PulsarSinkSettings(config)
    settings.kcql.head.getTarget shouldBe topic
    settings.kcql.head.getWithPartitioner shouldBe "RoundRobinPartition"
  }

  "should have messagemode CustomPartition" in {
    val config = PulsarSinkConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO $topic SELECT * FROM kafka_topic WITHPARTITIONER = CustomPartition",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings = PulsarSinkSettings(config)
    settings.kcql.head.getTarget shouldBe topic
    settings.kcql.head.getWithPartitioner shouldBe "CustomPartition"
  }

  "should have compression" in {
    val config = PulsarSinkConfig(
      Map(
        PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO $topic SELECT * FROM kafka_topic WITHCOMPRESSION = LZ4",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
      ).asJava,
    )

    val settings = PulsarSinkSettings(config)
    settings.kcql.head.getTarget shouldBe topic
    settings.kcql.head.getWithCompression shouldBe CompressionType.LZ4
  }
}

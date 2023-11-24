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
package io.lenses.streamreactor.connect.pulsar.source

import io.lenses.streamreactor.connect.converters.source.Converter
import io.lenses.streamreactor.connect.converters.source.JsonSimpleConverter
import io.lenses.streamreactor.connect.pulsar.config.PulsarConfigConstants
import io.lenses.streamreactor.connect.pulsar.config.PulsarSourceConfig
import io.lenses.streamreactor.connect.pulsar.config.PulsarSourceSettings
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scala.jdk.CollectionConverters.MapHasAsJava

/**
  * Created by andrew@datamountaineer.com on 24/01/2018.
  * stream-reactor
  */
class PulsarSourceTaskTest extends AnyWordSpec with Matchers {

  val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

  "should start create a task with a converter" in {
    val kcql =
      s"INSERT INTO kafka_topic SELECT * FROM $pulsarTopic BATCH = 10 WITHCONVERTER=`io.lenses.streamreactor.connect.converters.source.JsonSimpleConverter` WITHSUBSCRIPTION = SHARED"

    val props = Map(
      PulsarConfigConstants.HOSTS_CONFIG                   -> "pulsar://localhost:6650",
      PulsarConfigConstants.KCQL_CONFIG                    -> s"$kcql",
      PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
      PulsarConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
    ).asJava

    val config   = PulsarSourceConfig(props)
    val settings = PulsarSourceSettings(config, 1)

    val task = new PulsarSourceTask()
    val converters: Map[String, Converter] = task.buildConvertersMap(props, settings)
    converters.size shouldBe 1
    converters.head._2.getClass shouldBe classOf[JsonSimpleConverter]
  }

}

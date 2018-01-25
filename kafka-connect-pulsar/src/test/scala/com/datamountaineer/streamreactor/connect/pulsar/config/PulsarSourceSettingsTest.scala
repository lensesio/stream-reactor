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

package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.streamreactor.connect.converters.source.{AvroConverter, BytesConverter}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class PulsarSourceSettingsTest extends WordSpec with Matchers {
  "PulsarSourceSetting" should {

    val pulsarTopic = "persistent://landoop/standalone/connect/kafka-topic"

    "create an instance of settings" in {
      val config = PulsarSourceConfig(Map(
        PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kTopic SELECT * FROM $pulsarTopic WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
      ))
      val settings = PulsarSourceSettings(config, 1)

      settings.sourcesToConverters shouldBe Map(pulsarTopic -> classOf[AvroConverter].getCanonicalName)
      settings.throwOnConversion shouldBe true
      settings.pollingTimeout shouldBe 500
      settings.connection shouldBe "pulsar://localhost:6650"
    }

    "converted defaults to BytesConverter if not provided" in {
      val config = PulsarSourceConfig(Map(
        PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
        PulsarConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM pulsarSource",
        PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
        PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
      ))
      val settings = PulsarSourceSettings (config, 1)
      settings.sourcesToConverters shouldBe Map("pulsarSource" -> classOf[BytesConverter].getCanonicalName)
    }

    "throw an config exception if no kcql is set" in {
      intercept[ConfigException] {
        PulsarSourceConfig(Map(
          PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
      }
    }

    "throw an config exception if HOSTS_CONFIG is not defined" in {
      intercept[ConfigException] {
        val config = PulsarSourceConfig(Map(
          PulsarConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM pulsarSource",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
        PulsarSourceSettings(config, 1)
      }
    }

    "throw an config exception if the converter class can't be found" in {
      intercept[ConfigException] {
        PulsarSourceConfig(Map(
          PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kTopic SELECT * FROM pulsarSource WITHCONVERTER=`com.non.existance.SomeConverter`",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
      }
    }

    "throw an config exception if the converter settings with invalid source" in {
      intercept[ConfigException] {
        PulsarSourceConfig(Map(
          PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kTopic SELECT * FROM pulsarSource WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
      }
    }

    "throw an config exception if the converter topic doesn't match the KCQL settings" in {
      intercept[ConfigException] {
        PulsarSourceConfig(Map(
          PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kTopic SELECT * FROM pulsarSource WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
      }
    }

    "throw an config exception if exclusive and max tasks > 1" in {
      intercept[ConfigException] {
        val config = PulsarSourceConfig(Map(
          PulsarConfigConstants.HOSTS_CONFIG -> "pulsar://localhost:6650",
          PulsarConfigConstants.KCQL_CONFIG -> s"INSERT INTO kTopic SELECT * FROM $pulsarTopic WITHSUBSCRIPTION = exclusive",
          PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          PulsarConfigConstants.POLLING_TIMEOUT_CONFIG -> "500"
        ))
        val settings = PulsarSourceSettings(config, 2)
      }
    }
  }
}

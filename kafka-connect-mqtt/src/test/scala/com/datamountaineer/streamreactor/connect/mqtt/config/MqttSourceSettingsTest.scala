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
package io.lenses.streamreactor.connect.mqtt.config

import io.lenses.streamreactor.connect.converters.source.AvroConverter
import io.lenses.streamreactor.connect.converters.source.BytesConverter
import org.apache.kafka.common.config.ConfigException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.MapHasAsJava

class MqttSourceSettingsTest extends AnyWordSpec with Matchers {
  "MqttSourceSetting" should {

    "create an instance of settings" in {
      val settings = MqttSourceSettings {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.HOSTS_CONFIG                   -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
            MqttConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kTopic SELECT * FROM mqttSource WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }
      settings.mqttQualityOfService shouldBe 1
      settings.sourcesToConverters shouldBe Map("mqttSource" -> classOf[AvroConverter].getCanonicalName)
      settings.throwOnConversion shouldBe true
      settings.cleanSession shouldBe true
      settings.clientId shouldBe "someid"
      settings.password shouldBe Some("somepassw")
      settings.user shouldBe Some("user")
      settings.keepAliveInterval shouldBe 1000
      settings.connectionTimeout shouldBe 1000
      settings.pollingTimeout shouldBe 500
      settings.connection shouldBe "mqtt://localhost:61612?wireFormat.maxFrameSize=100000"
    }

    "converted defaults to BytesConverter if not provided" in {
      val settings = MqttSourceSettings {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.HOSTS_CONFIG                   -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
            MqttConfigConstants.KCQL_CONFIG                    -> "INSERT INTO kTopic SELECT * FROM mqttSource",
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }

      settings.sourcesToConverters shouldBe Map("mqttSource" -> classOf[BytesConverter].getCanonicalName)
    }

    "throw an config exception if no kcql is set" in {
      intercept[ConfigException] {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.HOSTS_CONFIG                   -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }
    }

    "throw an config exception if QS is less than 0" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(
            Map(
              MqttConfigConstants.KCQL_CONFIG                    -> "INSERT INTO kTopic SELECT * FROM mqttSource",
              MqttConfigConstants.QS_CONFIG                      -> "-1",
              MqttConfigConstants.HOSTS_CONFIG                   -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
              MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
              MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
              MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
              MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
              MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
              MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
              MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
              MqttConfigConstants.USER_CONFIG                    -> "user",
            ).asJava,
          ),
        )
      }
    }

    "throw an config exception if QS is more than 2" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(
            Map(
              MqttConfigConstants.KCQL_CONFIG                    -> "INSERT INTO kTopic SELECT * FROM mqttSource",
              MqttConfigConstants.QS_CONFIG                      -> "3",
              MqttConfigConstants.HOSTS_CONFIG                   -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
              MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
              MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
              MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
              MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
              MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
              MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
              MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
              MqttConfigConstants.USER_CONFIG                    -> "user",
            ).asJava,
          ),
        )
      }
    }

    "throw an config exception if HOSTS_CONFIG is not defined" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(
            Map(
              MqttConfigConstants.KCQL_CONFIG                    -> "INSERT INTO kTopic SELECT * FROM mqttSource",
              MqttConfigConstants.QS_CONFIG                      -> "1",
              MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
              MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
              MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
              MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
              MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
              MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
              MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
              MqttConfigConstants.USER_CONFIG                    -> "user",
            ).asJava,
          ),
        )
      }
    }

    "throw an config exception if the converter class can't be found" in {
      intercept[ConfigException] {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kTopic SELECT * FROM mqttSource WITHCONVERTER=com.non.existance.SomeConverter",
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }
    }

    "throw an config exception if the converter settings with invalid source" in {
      intercept[ConfigException] {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kTopic SELECT * FROM mqttSource WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }
    }

    "throw an config exception if the converter topic doesn't match the KCQL settings" in {
      intercept[ConfigException] {
        MqttSourceConfig(
          Map(
            MqttConfigConstants.KCQL_CONFIG                    -> s"INSERT INTO kTopic SELECT * FROM mqttSource WITHCONVERTER=`${classOf[AvroConverter].getCanonicalName}`",
            MqttConfigConstants.QS_CONFIG                      -> "1",
            MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttConfigConstants.CLEAN_SESSION_CONFIG           -> "true",
            MqttConfigConstants.CLIENT_ID_CONFIG               -> "someid",
            MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG      -> "1000",
            MqttConfigConstants.POLLING_TIMEOUT_CONFIG         -> "500",
            MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG     -> "1000",
            MqttConfigConstants.PASSWORD_CONFIG                -> "somepassw",
            MqttConfigConstants.USER_CONFIG                    -> "user",
          ).asJava,
        )
      }
    }

  }
}

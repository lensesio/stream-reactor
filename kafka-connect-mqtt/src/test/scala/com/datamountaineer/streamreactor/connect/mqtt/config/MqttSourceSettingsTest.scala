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

package com.datamountaineer.streamreactor.connect.mqtt.config

import com.datamountaineer.streamreactor.connect.converters.source.{AvroConverter, BytesConverter}
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class MqttSourceSettingsTest extends WordSpec with Matchers {
  "MqttSourceSetting" should {

    "create an instance of settings" in {
      val settings = MqttSourceSettings {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.HOSTS_CONFIG -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
          MqttSourceConfigConstants.CONVERTER_CONFIG -> s"mqttSource=${classOf[AvroConverter].getCanonicalName}",
          MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
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
      settings.connection shouldBe "mqtt://localhost:61612?wireFormat.maxFrameSize=100000"
    }

    "converted defaults to BytesConverter if not provided" in {
      val settings = MqttSourceSettings {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.HOSTS_CONFIG -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
          MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
      }

      settings.sourcesToConverters shouldBe Map("mqttSource" -> classOf[BytesConverter].getCanonicalName)
    }

    "throw an config exception if no kcql is set" in {
      intercept[ConfigException] {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.HOSTS_CONFIG -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
      }
    }

    "throw an config exception if QS is less than 0" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(Map(
            MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
            MqttSourceConfigConstants.QS_CONFIG -> "-1",
            MqttSourceConfigConstants.HOSTS_CONFIG -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
            MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
            MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
            MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
            MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
            MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
            MqttSourceConfigConstants.USER_CONFIG -> "user"
          )))
      }
    }

    "throw an config exception if QS is more than 2" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(Map(
            MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
            MqttSourceConfigConstants.QS_CONFIG -> "3",
            MqttSourceConfigConstants.HOSTS_CONFIG -> "mqtt://localhost:61612?wireFormat.maxFrameSize=100000",
            MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
            MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
            MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
            MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
            MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
            MqttSourceConfigConstants.USER_CONFIG -> "user"
          )))
      }
    }

    "throw an config exception if HOSTS_CONFIG is not defined" in {
      intercept[ConfigException] {
        MqttSourceSettings(
          MqttSourceConfig(Map(
            MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
            MqttSourceConfigConstants.QS_CONFIG -> "1",
            MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
            MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
            MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
            MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
            MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
            MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
            MqttSourceConfigConstants.USER_CONFIG -> "user"
          )))
      }
    }

    "throw an config exception if the converter class can't be found" in {
      intercept[ConfigException] {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.CONVERTER_CONFIG -> "kTopic=com.non.existance.SomeConverter",
          MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
      }
    }

    "throw an config exception if the converter settings with invalid source" in {
      intercept[ConfigException] {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.CONVERTER_CONFIG -> s"kTopic=${classOf[AvroConverter].getCanonicalName}",
          MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
      }
    }

    "throw an config exception if the converter topic doesn't match the KCQL settings" in {
      intercept[ConfigException] {
        MqttSourceConfig(Map(
          MqttSourceConfigConstants.CONVERTER_CONFIG -> s"kTopicA=${classOf[AvroConverter].getCanonicalName}",
          MqttSourceConfigConstants.KCQL_CONFIG -> "INSERT INTO kTopic SELECT * FROM mqttSource",
          MqttSourceConfigConstants.QS_CONFIG -> "1",
          MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG -> "true",
          MqttSourceConfigConstants.CLEAN_SESSION_CONFIG -> "true",
          MqttSourceConfigConstants.CLIENT_ID_CONFIG -> "someid",
          MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG -> "1000",
          MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG -> "1000",
          MqttSourceConfigConstants.PASSWORD_CONFIG -> "somepassw",
          MqttSourceConfigConstants.USER_CONFIG -> "user"
        ))
      }
    }

  }
}

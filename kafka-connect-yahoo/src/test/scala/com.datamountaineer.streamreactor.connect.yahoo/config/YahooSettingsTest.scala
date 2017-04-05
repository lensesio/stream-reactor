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

package com.datamountaineer.streamreactor.connect.yahoo.config

import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class YahooSettingsTest extends WordSpec with Matchers with YahooSourceConfig {
  "YahooSettings" should {
    "throw an exception if the fx symbols are set but not the kafka topic for fx" in {
      intercept[ConfigException] {
        val props = Map(
          YahooConfigConstants.FX -> "USDGBP=",
          YahooConfigConstants.STOCKS -> "MSFT",
          YahooConfigConstants.STOCKS_KAFKA_TOPIC -> "stocks_topic",
          YahooConfigConstants.ERROR_POLICY -> "THROW",
          YahooConfigConstants.NBR_OF_RETRIES -> "1"
        )

        val config = new AbstractConfig(configDef, props)
        YahooSettings(config)
      }
    }

    "throw an exception if the fx symbols are not set but the kafka topic for fx is" in {
      intercept[ConfigException] {
        val props = Map(
          YahooConfigConstants.FX_KAFKA_TOPIC -> "fx_topic",
          YahooConfigConstants.STOCKS -> "MSFT",
          YahooConfigConstants.STOCKS_KAFKA_TOPIC -> "stocks_topic",
          YahooConfigConstants.ERROR_POLICY -> "THROW",
          YahooConfigConstants.NBR_OF_RETRIES -> "1"
        )

        val config = new AbstractConfig(configDef, props)
        YahooSettings(config)
      }
    }

    "throw an exception if the stocks/quotes symbols are set but not the kafka topic for stocks/quotes" in {
      intercept[ConfigException] {
        val props = Map(
          YahooConfigConstants.FX -> "USDGBP=",
          YahooConfigConstants.FX_KAFKA_TOPIC -> "topic",
          YahooConfigConstants.STOCKS -> "MSFT",
          YahooConfigConstants.ERROR_POLICY -> "THROW",
          YahooConfigConstants.NBR_OF_RETRIES -> "1"
        )

        val config = new AbstractConfig(configDef, props)
        YahooSettings(config)
      }
    }
    "throw an exception if the stocks/quotes symbols are not set but the kafka topic for stocks/quotes is" in {
      intercept[ConfigException] {
        val props = Map(
          YahooConfigConstants.FX -> "USDGBP=",
          YahooConfigConstants.FX_KAFKA_TOPIC -> "topic",
          YahooConfigConstants.STOCKS -> "MSFT",
          YahooConfigConstants.ERROR_POLICY -> "THROW",
          YahooConfigConstants.NBR_OF_RETRIES -> "1"
        )

        val config = new AbstractConfig(configDef, props)
        YahooSettings(config)
      }
    }

    "create the settings instance" in {
      val props = Map(
        YahooConfigConstants.FX -> "USDGBP=,USDGBP=,EURGBP=",
        YahooConfigConstants.FX_KAFKA_TOPIC -> "topic_fx",
        YahooConfigConstants.STOCKS -> "MSFT,GOOGL ,MSFT ",
        YahooConfigConstants.STOCKS_KAFKA_TOPIC -> "topic_stocks",
        YahooConfigConstants.ERROR_POLICY -> "THROW",
        YahooConfigConstants.NBR_OF_RETRIES -> "1",
        YahooConfigConstants.POLL_INTERVAL -> "1500"
      )

      val config = new AbstractConfig(configDef, props)
      val settings = YahooSettings(config)
      settings.pollInterval shouldBe 1500
      settings.errorPolicy.getClass shouldBe classOf[ThrowErrorPolicy]

      settings.fxQuotes.size shouldBe 2
      settings.fxQuotes.contains("USDGBP=") shouldBe true
      settings.fxQuotes.contains("EURGBP=") shouldBe true
      settings.fxKafkaTopic shouldBe Some("topic_fx")

      settings.stocks.size shouldBe 2
      settings.stocks.contains("MSFT") shouldBe true
      settings.stocks.contains("GOOGL") shouldBe true
      settings.stocksKafkaTopic shouldBe Some("topic_stocks")
    }
  }
}

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

import java.util.logging.Logger

import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import org.apache.kafka.common.config.{AbstractConfig, ConfigException}


case class YahooSourceSetting(stocks: Set[String],
                              stocksKafkaTopic: Option[String],
                              fxQuotes: Set[String],
                              fxKafkaTopic: Option[String],
                              config: AbstractConfig,
                              pollInterval: Long = YahooConfigConstants.DEFAULT_POLL_INTERVAL,
                              errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                              taskRetires: Int = 10,
                              bufferSize: Int = YahooConfigConstants.DEFAULT_BUFFER_SIZE)

object YahooSettings {
  val logger: Logger = Logger.getLogger(getClass.getName)

  def apply(config: AbstractConfig): YahooSourceSetting = {

    val stocks = Option(config.getString(YahooConfigConstants.STOCKS))
      .map(v => v.split(",").map(_.trim.toUpperCase()).toSet)
      .getOrElse(Set.empty)

    val topicStocks = config.getString(YahooConfigConstants.STOCKS_KAFKA_TOPIC)
    if ((topicStocks == null && stocks.nonEmpty) || (topicStocks != null && stocks.isEmpty)) {
      throw new ConfigException(s"${YahooConfigConstants.STOCKS} and ${YahooConfigConstants.STOCKS_KAFKA_TOPIC} " +
        s"should be both set or left out")
    }

    val fx = Option(config.getString(YahooConfigConstants.FX)).map(v => v.split(",")
      .map(_.trim.toUpperCase()).toSet)
      .getOrElse(Set.empty)

    val topicFx = config.getString(YahooConfigConstants.FX_KAFKA_TOPIC)
    if ((topicFx == null && fx.nonEmpty) || (topicFx != null && fx.isEmpty)) {
      throw new ConfigException(s"${YahooConfigConstants.FX} and ${YahooConfigConstants.FX_KAFKA_TOPIC} should be both" +
        s"set or left out")
    }

    val pollInterval = config.getLong(YahooConfigConstants.POLL_INTERVAL)
    val errorPolicyValue = ErrorPolicyEnum.withName(config.getString(YahooConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyValue)

    val bufferSize = config.getInt(YahooConfigConstants.BUFFER_SIZE)

    val settings = YahooSourceSetting(stocks, Option(topicStocks), fx, Option(topicFx), config, pollInterval, errorPolicy, bufferSize)
    logger.info(
      s"""
         |Yahoo Source settings
         |$settings
      """.stripMargin)
    settings
  }
}

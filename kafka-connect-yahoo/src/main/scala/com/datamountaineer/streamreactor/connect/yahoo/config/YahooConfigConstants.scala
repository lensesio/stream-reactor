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

/**
  * Holds the constants used in the config.
  **/

object YahooConfigConstants {
  val DEFAULT_POLL_INTERVAL = 60000L
  //1 minute
  val Poll_Interval_Doc = "The polling interval between queries against yahoo webservice."
  val DEFAULT_ERROR_POLICY = "THROW"
  val DEFAULT_RETRIES = 10

  val POLL_INTERVAL = "connect.yahoo.source.poll.interval"
  val POLL_INTERVAL_DOC = "Specifies how often if polls Yahoo services for new data"

  val STOCKS = "connect.yahoo.source.stocks.subscriptions"
  val STOCKS_DOC = "Sets the financial's stocks to query from Yahoo finance."
  val STOCKS_KAFKA_TOPIC = "connect.yahoo.source.stocks.topic"
  val STOCKS_KAFKA_TOPIC_DOC = "Specifies which kafka topic will receive the the stock information"

  val FX = "connect.yahoo.source.fx.subscriptions"
  val FX_DOC = "Sets the foreign exchange values to retrieve from Yahoo finance"
  val FX_KAFKA_TOPIC = "connect.yahoo.source.fx.topic"
  val FX_KAFKA_TOPIC_DOC = "Specifies the target kafka topic to send the FX values to"

  val ERROR_POLICY = "connect.yahoo.source.error.policy"
  val ERROR_POLICY_DOC = "Sets the error handling approach. Available options are THROW and NOOP"

  val NBR_OF_RETRIES = "connect.yahoo.source.retries"
  val NBR_OF_RETRIES_DOC = ""

  val BUFFER_SIZE = "connect.yahoo.source.buffer.size"
  val BUFFER_SIZE_DOC = "How many records it should buffer as it gets more data from yahoo"
  val DEFAULT_BUFFER_SIZE = 10000
}

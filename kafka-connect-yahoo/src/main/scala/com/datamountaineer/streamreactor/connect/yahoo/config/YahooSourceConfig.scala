/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.yahoo.config

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}


/**
  * Holds the base configuration.
  **/
trait YahooSourceConfig {

  val configDef: ConfigDef = new ConfigDef()
    .define(YahooConfigConstants.FX,
      Type.STRING,
      Importance.HIGH,
      YahooConfigConstants.FX_DOC)

    .define(YahooConfigConstants.FX_KAFKA_TOPIC,
      Type.STRING,
      Importance.HIGH,
      YahooConfigConstants.FX_KAFKA_TOPIC_DOC)

    .define(YahooConfigConstants.STOCKS,
      Type.STRING,
      Importance.HIGH,
      YahooConfigConstants.STOCKS_DOC)

    .define(YahooConfigConstants.STOCKS_KAFKA_TOPIC,
      Type.STRING,
      Importance.HIGH,
      YahooConfigConstants.STOCKS_KAFKA_TOPIC_DOC)

    .define(YahooConfigConstants.ERROR_POLICY,
      Type.STRING,
      YahooConfigConstants.DEFAULT_ERROR_POLICY,
      Importance.HIGH,
      YahooConfigConstants.ERROR_POLICY_DOC
    )

    .define(YahooConfigConstants.NBR_OF_RETRIES,
      Type.INT,
      YahooConfigConstants.DEFAULT_RETRIES,
      Importance.MEDIUM,
      YahooConfigConstants.NBR_OF_RETRIES_DOC)


    .define(YahooConfigConstants.POLL_INTERVAL,
      Type.LONG,
      YahooConfigConstants.DEFAULT_POLL_INTERVAL,
      Importance.HIGH,
      YahooConfigConstants.POLL_INTERVAL_DOC)

    .define(YahooConfigConstants.BUFFER_SIZE,
      Type.INT,
      YahooConfigConstants.DEFAULT_BUFFER_SIZE,
      Importance.MEDIUM,
      YahooConfigConstants.BUFFER_SIZE_DOC)
}

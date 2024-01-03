/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging

import scala.jdk.CollectionConverters.MapHasAsScala

trait PubSubSupport extends StrictLogging {

  // How to 'score' each message
  def getChannelField(kcqlConfig: Kcql): String = {
    val pubSubParams = kcqlConfig.getStoredAsParameters.asScala
    val channelField =
      if (pubSubParams.keys.exists(k => k.equalsIgnoreCase("channel")))
        pubSubParams.find { case (k, _) => k.equalsIgnoreCase("channel") }.get._2
      else {
        logger.info("You have not defined a [channel] field. We'll try to fall back to [channel] field")
        "channel"
      }
    channelField
  }

//   assert(SS.isValid, "The SortedSet definition at Redis accepts only case sensitive alphabetic characters")

}

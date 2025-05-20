/*
 * Copyright 2017-2025 Lenses.io Ltd
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

trait SortedSetSupport extends StrictLogging {

  // How to 'score' each message
  def getScoreField(kcqlConfig: Kcql): String = {
    val sortedSetParams = kcqlConfig.getStoredAsParameters.asScala
    val scoreField =
      if (sortedSetParams.keys.exists(k => k.equalsIgnoreCase("score")))
        sortedSetParams.find { case (k, _) => k.equalsIgnoreCase("score") }.get._2
      else {
        logger.info("You have not defined how to 'score' each message. We'll try to fall back to 'timestamp' field")
        "timestamp"
      }
    scoreField
  }

  // assert(SS.isValid, "The SortedSet definition at Redis accepts only case sensitive alphabetic characters")

}

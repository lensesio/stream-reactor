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
package io.lenses.streamreactor.common.config

import io.lenses.kcql.Kcql
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

/**
  * Created by andrew@datamountaineer.com on 13/05/16.
  * kafka-connect-common
  */

object Helpers extends StrictLogging {

  def checkInputTopics(kcqlConstant: String, props: Map[String, String]): Boolean = {
    val topics = props("topics").split(",").map(t => t.trim).toSet
    val raw    = props(kcqlConstant)
    if (raw.isEmpty) {
      throw new ConfigException(s"Missing $kcqlConstant")
    }
    val kcql    = raw.split(";").map(r => Kcql.parse(r)).toSet
    val sources = kcql.map(k => k.getSource)
    val res     = topics.subsetOf(sources)

    if (!res) {
      val missing = topics.diff(sources)
      throw new ConfigException(
        s"Mandatory `topics` configuration contains topics not set in $kcqlConstant: ${missing}, kcql contains $sources",
      )
    }

    val res1 = sources.subsetOf(topics)

    if (!res1) {
      val missing = topics.diff(sources)
      throw new ConfigException(
        s"$kcqlConstant configuration contains topics not set in mandatory `topic` configuration: ${missing}, kcql contains $sources",
      )
    }

    true
  }
}

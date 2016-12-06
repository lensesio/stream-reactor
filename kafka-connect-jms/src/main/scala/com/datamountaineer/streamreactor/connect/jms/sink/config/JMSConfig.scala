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

package com.datamountaineer.streamreactor.connect.jms.sink.config

import com.datamountaineer.connector.config.{Config, FormatType}
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.ConfigException

import scala.collection.JavaConversions._

case class JMSConfig(destinationType: DestinationType,
                     source: String,
                     target: String,
                     includeAllFields: Boolean,
                     fieldsAlias: Map[String, String],
                     format: FormatType = FormatType.JSON
                    )

object JMSConfig extends StrictLogging {

  def apply(config: Config, destinationType: DestinationType): JMSConfig = {
    new JMSConfig(
      destinationType,
      config.getSource,
      config.getTarget,
      config.isIncludeAllFields,
      config.getFieldAlias.map(f => f.getField -> f.getAlias).toMap,
      getFormatType(config))
  }

  def apply(config: Config, topics: Set[String], queues: Set[String]): JMSConfig = {
    new JMSConfig(
      getDestinationType(config.getTarget, queues, topics),
      config.getSource,
      config.getTarget,
      config.isIncludeAllFields,
      config.getFieldAlias.map(f => f.getField -> f.getAlias).toMap,
      getFormatType(config))
  }

  def getFormatType(config: Config) : FormatType = {
    val format = Option(config.getFormatType)
    format.getOrElse(FormatType.JSON)
  }

  def getDestinationType(target: String, queues: Set[String], topics: Set[String]): DestinationType = {
    if (topics.contains(target)) {
      TopicDestination
    } else if (queues.contains(target)) {
      QueueDestination
    } else {
      throw new ConfigException(s"$target has not been configured as topic or queue.")
    }
  }
}

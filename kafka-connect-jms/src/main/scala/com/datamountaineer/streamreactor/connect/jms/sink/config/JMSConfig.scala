package com.datamountaineer.streamreactor.connect.jms.sink.config

import com.datamountaineer.connector.config.Config
import io.confluent.common.config.ConfigException

import scala.collection.JavaConversions._

case class JMSConfig(destinationType: DestinationType,
                     source: String,
                     target: String,
                     includeAllFields: Boolean,
                     fieldsAlias: Map[String, String])

object JMSConfig {

  def apply(config: Config, destinationType: DestinationType): JMSConfig = {
    new JMSConfig(
      destinationType,
      config.getSource,
      config.getTarget,
      config.isIncludeAllFields,
      config.getFieldAlias.map(f => f.getField -> f.getAlias).toMap)
  }

  def apply(config: Config, topics: Set[String], queues: Set[String]): JMSConfig = {
    new JMSConfig(
      getDestinationType(config.getTarget, queues, topics),
      config.getSource,
      config.getTarget,
      config.isIncludeAllFields,
      config.getFieldAlias.map(f => f.getField -> f.getAlias).toMap)
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
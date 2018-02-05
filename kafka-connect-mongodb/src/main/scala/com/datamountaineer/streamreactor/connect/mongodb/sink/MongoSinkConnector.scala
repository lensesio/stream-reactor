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

package com.datamountaineer.streamreactor.connect.mongodb.sink

import java.util

import com.datamountaineer.streamreactor.connect.config.Helpers
import com.datamountaineer.streamreactor.connect.mongodb.config.{MongoConfig, MongoConfigConstants}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

/**
  * <h1>Mongo SinkConnector</h1>
  * Kafka connect Mongo Sink connector
  *
  * Sets up MongoSinkTask and configurations for the tasks.
  **/
class MongoSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: util.Map[String, String] = _
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)


  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[MongoSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")

    val kcql = configProps.get(MongoConfigConstants.KCQL_CONFIG).split(";")
    if (maxTasks == 1 || kcql.length == 1) {
      List(configProps)
    }
    else {
      val groups = kcql.length / maxTasks + kcql.length % maxTasks
      kcql.grouped(groups)
        .map(_.mkString(";"))
        .map { routes =>
          val taskProps = new util.HashMap[String, String](configProps)
          taskProps.put(MongoConfigConstants.KCQL_CONFIG, routes)
          taskProps
        }.toList
    }
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    Helpers.checkInputTopics(MongoConfigConstants.KCQL_CONFIG, props.asScala.toMap)
    Try(MongoConfig(props)) match {
      case Failure(f) => throw new ConnectException(s"Couldn't start Mongo sink due to configuration error: ${f.getMessage}", f)
      case _ =>
    }

    configProps = props
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = MongoConfig.config
}

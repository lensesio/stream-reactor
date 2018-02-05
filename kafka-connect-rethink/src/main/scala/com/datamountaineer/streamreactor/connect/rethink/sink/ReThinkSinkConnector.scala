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

package com.datamountaineer.streamreactor.connect.rethink.sink

import java.util

import com.datamountaineer.streamreactor.connect.config.Helpers
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkConfigConstants, ReThinkSinkConfig, ReThinkSinkSettings}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.rethinkdb.RethinkDB
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._


/**
  * Created by andrew@datamountaineer.com on 24/03/16. 
  * stream-reactor
  */
class ReThinkSinkConnector extends SinkConnector with StrictLogging {
  private var configProps: util.Map[String, String] = _
  private val configDef = ReThinkSinkConfig.config
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[ReThinkSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps).toList.asJava
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting ReThinkDB sink connector")
    Helpers.checkInputTopics(ReThinkConfigConstants.KCQL, props.asScala.toMap)

    /**
      * ReThinkDb allows creation of tables with the same name at the same time
      * Moved the table creation here, it means we parse the config twice as we
      * need the target and primary keys from KCQL.
      **/
    val rethink = RethinkDB.r
    initializeTables(rethink, props)
    configProps = props
  }

  def initializeTables(rethink: RethinkDB, props: util.Map[String, String]): Unit = {
    val config = ReThinkSinkConfig(props)
    val settings = ReThinkSinkSettings(config)
    val rethinkHost = config.getString(ReThinkConfigConstants.RETHINK_HOST)
    val port = config.getInt(ReThinkConfigConstants.RETHINK_PORT)

    val conn = rethink.connection().hostname(rethinkHost).port(port).connect()
    ReThinkHelper.checkAndCreateTables(rethink, settings, conn)
    conn.close()
  }

  override def stop(): Unit = {}

  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}

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

package com.datamountaineer.streamreactor.connect.cassandra.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.cassandra.CassandraConnection
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings, CassandraSourceSetting}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */
class CassandraSourceTask extends SourceTask with StrictLogging {
  private var queues = mutable.Map.empty[String, LinkedBlockingQueue[SourceRecord]]
  private val readers = mutable.Map.empty[String, CassandraTableReader]
  private var taskConfig : Option[AbstractConfig] = None
  private var connection : Option[CassandraConnection] = None
  private var settings : Set[CassandraSourceSetting] = Set.empty
  private var bufferSize : Option[Int] = None
  private var batchSize : Option[Int] = None


  /**
    * Starts the Cassandra source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    */
  override def start(props: util.Map[String, String]): Unit = {

    //get configuration for this task
    taskConfig = Try(new CassandraConfigSource(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    //ascii art!!
    logger.info("""
                  |
                  |    ____        __        __  ___                  __        _
                  |   / __ \____ _/ /_____ _/  |/  /___  __  ______  / /_____ _(_)___  ___  ___  _____
                  |  / / / / __ `/ __/ __ `/ /|_/ / __ \/ / / / __ \/ __/ __ `/ / __ \/ _ \/ _ \/ ___/
                  | / /_/ / /_/ / /_/ /_/ / /  / / /_/ / /_/ / / / / /_/ /_/ / / / / /  __/  __/ /
                  |/_____/\__,_/\__/\__,_/_/  /_/\____/\__,_/_/ /_/\__/\__,_/_/_/ /_/\___/\___/_/
                  |       ______                                __           _____
                  |      / ____/___ _______________ _____  ____/ /________ _/ ___/____  __  _______________
                  |     / /   / __ `/ ___/ ___/ __ `/ __ \/ __  / ___/ __ `/\__ \/ __ \/ / / / ___/ ___/ _ \
                  |    / /___/ /_/ (__  |__  ) /_/ / / / / /_/ / /  / /_/ /___/ / /_/ / /_/ / /  / /__/  __/
                  |    \____/\__,_/____/____/\__,_/_/ /_/\__,_/_/   \__,_//____/\____/\__,_/_/   \___/\___/
                  |
                  | By Andrew Stevenson.""".stripMargin)

    //get the list of assigned tables this sink
    val assigned = taskConfig.get.getString(CassandraConfigConstants.ASSIGNED_TABLES).split(",").toList
    bufferSize = Some(taskConfig.get.getInt(CassandraConfigConstants.READER_BUFFER_SIZE))
    batchSize = Some(taskConfig.get.getInt(CassandraConfigConstants.BATCH_SIZE))

    ///get cassandra connection
    connection = Try(CassandraConnection(taskConfig.get)) match {
      case Success(s) =>
        logger.info("Connection to Cassandra established.")
        Some(s)
      case Failure(f) => throw new ConnectException(s"Couldn't connect to Cassandra.", f)
    }

    //Setup queues for readers to put records into
    assigned.map(table => queues += table -> new LinkedBlockingQueue[SourceRecord](bufferSize.get))
    settings = CassandraSettings.configureSource(taskConfig.get)

    //set up readers
    assigned.map(table => {
      //get settings
      val setting = settings.filter(s=>s.routes.getSource.equals(table)).head
      val session = connection.get.session
      val queue = queues(table)
      readers += table -> CassandraTableReader(session = session, setting = setting, context = context, queue = queue)
    })
  }

  /**
    * Called by the Framework
    *
    * Map over the queues, draining each and returning SourceRecords.
    *
    * @return A util.List of SourceRecords.
    */
  override def poll(): util.List[SourceRecord] = {
    settings.map(s=>s.routes).flatten(r=>process(r.getSource)).toList
  }

  /**
    * Process the table
    *
    * Get the reader can call read, if querying it will return then drain it's queue
    * else if will start a new query then drain.
    *
    * @param table The table to query and drain
    * @return A list of Source records
    */
  def process(table: String) : List[SourceRecord]= {
    val reader = readers(table)
    reader.read()
    val queue = queues(table)
    if (!queue.isEmpty) {
      QueueHelpers.drainQueue(queues(table), batchSize.get).toList
    } else {
      List[SourceRecord]()
    }
  }

  /**
    * Stop the task and close readers.
    */
  override def stop(): Unit = {
    logger.info("Stopping Cassandra source.")
    readers.foreach( {case (k, v) => v.close()})
    val cluster = connection.get.session.getCluster
    logger.info("Shutting down Cassandra driver connections.")
    connection.get.session.close()
    cluster.close()
  }

  /**
    * Gets the version of this Source.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion


  /**
    * Check if the reader for a table is querying.
    *
    * @param table The table to check if a query is active against.
    * @return A boolean indicating if querying is in progress.
    */
  private[datamountaineer] def isReaderQuerying(table: String) : Boolean = {
    readers(table).isQuerying
  }

  /**
    * Check the queue size of a table.
    *
    * @param table The table to check the queue size for.
    * @return The size of the queue.
    */
  private[datamountaineer] def queueSize(table: String) : Int = {
    queues(table).size()
  }
}

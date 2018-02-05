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
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */


class CassandraSourceTask extends SourceTask with StrictLogging {
  private var queues = mutable.Map.empty[String, LinkedBlockingQueue[SourceRecord]]
  private val readers = mutable.Map.empty[String, CassandraTableReader]
  private var taskConfig: Option[CassandraConfigSource] = None
  private var connection: Option[CassandraConnection] = None
  private var settings: Seq[CassandraSourceSetting] = _
  private var bufferSize: Option[Int] = None
  private var batchSize: Option[Int] = None
  private var tracker: Long = 0
  private var pollInterval: Long = CassandraConfigConstants.DEFAULT_POLL_INTERVAL
  private var name: String = ""
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)


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

    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/cass-source-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

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
    name = props.getOrDefault("name", "")

    //set up readers
    assigned.map(table => {
      //get settings
      val setting = settings.filter(s => s.kcql.getSource.equals(table)).head
      val session = connection.get.session
      val queue = queues(table)
      readers += table -> CassandraTableReader(name = name, session = session, setting = setting, context = context, queue = queue)
    })

    pollInterval = settings.head.pollInterval
  }

  /**
    * Called by the Framework
    *
    * Map over the queues, draining each and returning SourceRecords.
    *
    * @return A util.List of SourceRecords.
    */
  override def poll(): util.List[SourceRecord] = {
    val now = System.currentTimeMillis()
    if (tracker + pollInterval <= now) {
      tracker = now
      settings
        .map(s => s.kcql)
        .flatten(r => process(r.getSource))
        .toList
    } else {
      logger.debug(s"Waiting for poll interval to pass")
      Thread.sleep(pollInterval)
      List[SourceRecord]()
    }
  }


  /**
    * Process the table
    *
    * Get the reader can call read, if querying it will return then drain it's queue
    * else if will start a new query then drain.
    *
    * @param tableName The table to query and drain
    * @return A list of Source records
    */
  def process(tableName: String): List[SourceRecord] = {
    val reader = readers(tableName)
    val queue = queues(tableName)
    
    logger.info(s"Connector $name start of poll queue size for $tableName is: ${queue.size}")

    // minimized the changes but still fix #300
    // depending on buffer size, batch size, and volume of data coming in
    // we could see data on the internal queue coming off slower
    // than we are putting data into it

    if (!reader.isQuerying) {
      // start another query 
      reader.read
    } else {
      // if we are in the middle of working 
      // on data from the last polling cycle
      // we will not attempt to get more data
      logger.info(s"Connector $name reader for table $tableName is still querying...")
    }
    
    // let's make some of the data on the queue 
    // available for publishing to Kafka
    val records = if (!queue.isEmpty) {
      logger.info(s"Connector $name attempting to drain $batchSize items from the queue for table $tableName")
      QueueHelpers.drainQueue(queue, batchSize.get).toList
    } else {
      List[SourceRecord]()
    }
    
    logger.info(s"Connector $name end of poll queue size for $tableName is: ${queue.size}")
    
    records
  }

  /**
    * Stop the task and close readers.
    */
  override def stop(): Unit = {
    logger.info(s"Stopping Cassandra source $name.")
    readers.foreach({ case (_, v) => v.close() })
    val cluster = connection.get.session.getCluster
    logger.info(s"Shutting down Cassandra driver connections for $name.")
    connection.get.session.close()
    cluster.close()
  }

  /**
    * Gets the version of this Source.
    *
    * @return
    */
  override def version: String = manifest.version()


  /**
    * Check if the reader for a table is querying.
    *
    * @param table The table to check if a query is active against.
    * @return A boolean indicating if querying is in progress.
    */
  private[datamountaineer] def isReaderQuerying(table: String): Boolean = {
    readers(table).isQuerying
  }

  /**
    * Check the queue size of a table.
    *
    * @param table The table to check the queue size for.
    * @return The size of the queue.
    */
  private[datamountaineer] def queueSize(table: String): Int = {
    queues(table).size()
  }
}

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
package io.lenses.streamreactor.connect.cassandra.source

import io.lenses.streamreactor.common.queues.QueueHelpers
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.util.JarManifest

import java.util
import java.util.concurrent.LinkedBlockingQueue
import io.lenses.streamreactor.connect.cassandra.CassandraConnection
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigConstants
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigSource
import io.lenses.streamreactor.connect.cassandra.config.CassandraSettings
import io.lenses.streamreactor.connect.cassandra.config.CassandraSourceSetting
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import scala.collection.mutable
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 14/04/16.
  * stream-reactor
  */

class CassandraSourceTask extends SourceTask with StrictLogging {
  private val queues      = mutable.Map.empty[String, LinkedBlockingQueue[SourceRecord]]
  private val readers     = mutable.Map.empty[String, CassandraTableReader]
  private val stopControl = new Object()
  private var taskConfig:   Option[CassandraConfigSource] = None
  private var connection:   Option[CassandraConnection]   = None
  private var settings:     Seq[CassandraSourceSetting]   = _
  private var bufferSize:   Option[Int]                   = None
  private var batchSize:    Option[Int]                   = None
  private var tracker:      Long                          = 0
  private var pollInterval: Long                          = CassandraConfigConstants.DEFAULT_POLL_INTERVAL
  private var name:         String                        = ""
  private val manifest =  new JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Starts the Cassandra source, parsing the options and setting up the reader.
    *
    * @param props A map of supplied properties.
    */
  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/cass-source-ascii.txt")

    val config = if (context.configs().isEmpty) props else context.configs()

    //get configuration for this task
    taskConfig = Try(new CassandraConfigSource(config.asScala.toMap)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSource due to configuration error.", f)
      case Success(s) => Some(s)
    }

    //get the list of assigned tables this sink
    val assigned = taskConfig.get.getString(CassandraConfigConstants.ASSIGNED_TABLES).split(",").toList
    bufferSize = Some(taskConfig.get.getInt(CassandraConfigConstants.READER_BUFFER_SIZE))
    batchSize  = Some(taskConfig.get.getInt(CassandraConfigConstants.BATCH_SIZE))

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
    name     = props.getOrDefault("name", "")

    //set up readers
    assigned.map { table =>
      //get settings
      val setting = settings.filter(s => s.kcql.getSource.equals(table)).head
      val session = connection.get.session
      val queue   = queues(table)
      readers += table -> CassandraTableReader(name = name,
                                               session = session,
                                               setting = setting,
                                               context = context,
                                               queue   = queue,
      )
    }

    pollInterval = settings.head.pollInterval
  }

  /**
    * Called by the Framework
    *
    * Map over the queues, draining each and returning SourceRecords.
    *
    * @return A util.List of SourceRecords.
    */
  override def poll(): util.List[SourceRecord] =
    settings
      .map(s => s.kcql)
      .flatten(r => process(r.getSource))
      .toList.asJava

  /**
    * Waiting Poll Interval
    */
  private def waitPollInterval() = {
    val now = System.currentTimeMillis()
    if (tracker + pollInterval <= now) {
      tracker = now
    } else {
      logger.debug(s"Waiting for poll interval to pass")
      stopControl.synchronized {
        stopControl.wait(pollInterval)
      }
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
    val queue  = queues(tableName)

    logger.debug(s"Connector $name start of poll queue size for $tableName is: ${queue.size}")

    // minimized the changes but still fix #300
    // depending on buffer size, batch size, and volume of data coming in
    // we could see data on the internal queue coming off slower
    // than we are putting data into it

    if (!reader.isQuerying) {
      waitPollInterval()
      // start another query
      reader.read()
    } else {
      // if we are in the middle of working
      // on data from the last polling cycle
      // we will not attempt to get more data
      logger.debug(s"Connector $name reader for table $tableName is still querying...")
    }

    // let's make some of the data on the queue
    // available for publishing to Kafka
    val records = if (!queue.isEmpty) {
      val sendSize = if (queue.size() > batchSize.get) batchSize.get else queue.size()
      logger.info(s"Sending $sendSize records for connector $name")
      QueueHelpers.drainQueue(queue, batchSize.get).asScala.toList
    } else {
      List[SourceRecord]()
    }

    logger.debug(s"Connector $name end of poll queue size for $tableName is: ${queue.size}")

    records
  }

  /**
    * Stop the task and close readers.
    */
  override def stop(): Unit = {
    logger.info(s"Stopping Cassandra source $name.")
    stopControl.synchronized {
      stopControl.notifyAll
    }
    readers.foreach({ case (_, v) => v.close() })

    logger.info(s"Shutting down Cassandra driver connections for $name.")
    connection.foreach { c =>
      c.session.close()
      c.cluster.close()
    }
  }

  /**
    * Gets the version of this Source.
    *
    * @return
    */
  override def version: String = manifest.getVersion()

  /**
    * Check the queue size of a table.
    *
    * @param table The table to check the queue size for.
    * @return The size of the queue.
    */
  private[lenses] def queueSize(table: String): Int =
    queues(table).size()
}

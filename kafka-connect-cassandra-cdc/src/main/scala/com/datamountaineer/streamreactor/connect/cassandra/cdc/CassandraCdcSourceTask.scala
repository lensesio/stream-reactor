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
package com.datamountaineer.streamreactor.connect.cassandra.cdc

import java.net.ServerSocket
import java.util

import com.datamountaineer.streamreactor.connect.cassandra.cdc.config.{CassandraConnect, CdcConfig}
import com.datamountaineer.streamreactor.connect.cassandra.cdc.io.MachineNameFn
import com.datamountaineer.streamreactor.connect.cassandra.cdc.logs.{CdcCassandra, Offset}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.util.{Failure, Success, Try}

class CassandraCdcSourceTask extends SourceTask with StrictLogging {
  private var taskConfig: Option[CassandraConnect] = _
  private var settings: Seq[CdcConfig] = _

  private var cdc: CdcCassandra = _
  private var socketServer: ServerSocket = _

  private val emptyRecords = new util.ArrayList[SourceRecord]()

  /**
    * Starts the Cassandra CDC source, parsing the options and starts to monitor the CDC folder for new files.
    *
    * @param props A map of supplied properties.
    */
  override def start(props: util.Map[String, String]): Unit = {

    //get configuration for this task
    taskConfig = Try(new CassandraConnect(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Cassandra CDC Source due to configuration error.", f)
      case Success(s) =>
        val singleInstancePort = s.getInt(CassandraConnect.SINGLE_INSTANCE_PORT)
        val machineName = Try(MachineNameFn()).getOrElse("Unknown")
        try {
          logger.info(s"Acquiring port $singleInstancePort to enforce single instance being run on $machineName")
          socketServer = new ServerSocket(singleInstancePort)
          Some(s)
        }
        catch {
          case t: Throwable =>
            logger.warn(s"Cassandra Connect CDC is already running on this machine:$machineName.This task allthough running will never do any work")
            None
        }
    }

    taskConfig.foreach { c =>
      logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/cass-source-ascii.txt")).mkString)
      implicit val conf = CdcConfig(c)
      cdc = new CdcCassandra()

      //getting the previous state if any
      val state: util.Map[String, AnyRef] = Option(context).map(_.offsetStorageReader()).map(_.offset(ConnectState.Key)).orNull
      val offset = Offset.from(state)
      cdc.start(offset)
    }
  }

  def isActuallyRunning: Boolean = cdc != null

  /**
    * Called by the Framework
    *
    * Map over the queues, draining each and returning SourceRecords.
    *
    * @return A util.List of SourceRecords.
    */
  override def poll(): util.List[SourceRecord] = {
    val now = System.currentTimeMillis()
    emptyRecords
  }


  /**
    * Stop the task and close readers.
    */
  override def stop(): Unit = {
    logger.info("Stopping Cassandra CDC source...")
    Option(cdc).foreach(_.close())
    Option(socketServer).foreach(_.close())
    logger.info("Cassandra CDC source stopped.")
  }

  /**
    * Gets the version of this Source.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion
}

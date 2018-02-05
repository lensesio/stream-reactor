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

package com.datamountaineer.streamreactor.connect.cassandra.sink

import java.util

import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigSink, CassandraSettings}
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}


/**
  * <h1>CassandraSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by
  * framework to put records to the target sink
  **/
class CassandraSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[CassandraJsonWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  logger.info("Task initialising")


  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    val taskConfig = Try(new CassandraConfigSink(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSink due to configuration error.", f)
      case Success(s) => s
    }

    val sinkSettings = CassandraSettings.configureSink(taskConfig)
    enableProgress = sinkSettings.enableProgress
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/cass-sink-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())
    writer = Some(CassandraWriter(connectorConfig = taskConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(w => w.write(seq))
    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up Cassandra connections
    **/
  override def stop(): Unit = {
    logger.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
    if (enableProgress) {
      progressCounter.empty
    }
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def version: String = manifest.version()
}

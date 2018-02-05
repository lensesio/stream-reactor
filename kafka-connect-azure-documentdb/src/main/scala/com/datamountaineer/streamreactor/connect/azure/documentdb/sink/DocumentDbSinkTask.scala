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

package com.datamountaineer.streamreactor.connect.azure.documentdb.sink

import java.util

import com.datamountaineer.streamreactor.connect.azure.documentdb.DocumentClientProvider
import com.datamountaineer.streamreactor.connect.azure.documentdb.config.{DocumentDbConfig, DocumentDbConfigConstants, DocumentDbSinkSettings}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.microsoft.azure.documentdb.DocumentClient
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * <h1>DocumentSinkTask</h1>
  *
  * Kafka Connect Azure Document DB sink task. Called by
  * framework to put records to the target sink
  **/
class DocumentDbSinkTask private[sink](val builder: DocumentDbSinkSettings => DocumentClient) extends SinkTask with StrictLogging {
  private var writer: Option[DocumentDbWriter] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false

  def this() = this(DocumentClientProvider.get)

  /**
    * Parse the configurations and setup the writer
    **/
  override def start(props: util.Map[String, String]): Unit = {
    val taskConfig = Try(DocumentDbConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Azure Document DB Sink due to configuration error.", f)
      case Success(s) => s
    }

    logger.info(scala.io.Source.fromInputStream(this.getClass.getResourceAsStream("/documentdb-sink-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    implicit val settings = DocumentDbSinkSettings(taskConfig)
    //if error policy is retry set retry interval
    if (settings.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(taskConfig.getLong(DocumentDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG))
    }

    logger.info(s"Initialising Document Db writer.")
    writer = Some(new DocumentDbWriter(settings, builder(settings)))
    enableProgress = taskConfig.getBoolean(DocumentDbConfigConstants.PROGRESS_COUNTER_ENABLED)
  }

  /**
    * Pass the SinkRecords to the Azure Document DB writer for storing them
    **/
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(w => w.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = {
    logger.info("Stopping Azure Document DB sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def version: String = manifest.version()
}

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

package com.datamountaineer.streamreactor.connect.coap.source

import java.util
import java.util.concurrent.LinkedBlockingQueue

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSettings, CoapSourceConfig}
import com.datamountaineer.streamreactor.connect.queues.QueueHelpers
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
class CoapSourceTask extends SourceTask with StrictLogging {
  private var readers: Set[CoapReader] = _
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val queue = new LinkedBlockingQueue[SourceRecord]()
  private var batchSize: Int = CoapConstants.BATCH_SIZE_DEFAULT
  private var lingerTimeout = CoapConstants.SOURCE_LINGER_MS_DEFAULT
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/coap-source-ascii.txt")).mkString + s" $version")
    logger.info(manifest.printManifest())

    val conf = if (context.configs().isEmpty) props else context.configs()

    val config = CoapSourceConfig(conf)
    enableProgress = config.getBoolean(CoapConstants.PROGRESS_COUNTER_ENABLED)
    val settings = CoapSettings(config)
    batchSize = config.getInt(CoapConstants.BATCH_SIZE)
    lingerTimeout = config.getInt(CoapConstants.SOURCE_LINGER_MS)
    enableProgress = config.getBoolean(CoapConstants.PROGRESS_COUNTER_ENABLED)
    readers = CoapReaderFactory(settings, queue)
  }

  override def poll(): util.List[SourceRecord] = {
    val records = new util.ArrayList[SourceRecord]()

    QueueHelpers.drainWithTimeoutNoGauva(records, batchSize, lingerTimeout * 1000000 , queue)

    if (enableProgress) {
      progressCounter.update(records.asScala.toVector)
    }
    records
  }

  override def stop(): Unit = {
    logger.info("Stopping Coap source and closing connections.")
    readers.foreach(_.stop())
    progressCounter.empty
  }

  override def version: String = manifest.version()
}

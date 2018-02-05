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

package com.datamountaineer.streamreactor.connect.coap.sink

import java.util

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSettings, CoapSinkConfig}
import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.utils.{ProgressCounter, JarManifest}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by andrew@datamountaineer.com on 29/12/2016. 
  * stream-reactor
  */
class CoapSinkTask extends SinkTask with StrictLogging {
  private val writers = mutable.Map.empty[String, CoapWriter]
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/coap-sink-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())
    val sinkConfig = CoapSinkConfig(props)
    enableProgress = sinkConfig.getBoolean(CoapConstants.PROGRESS_COUNTER_ENABLED)
    val settings = CoapSettings(sinkConfig)

    //if error policy is retry set retry interval
    if (settings.head.errorPolicy.equals(Option(ErrorPolicyEnum.RETRY))) {
      context.timeout(sinkConfig.getString(CoapConstants.ERROR_RETRY_INTERVAL).toLong)
    }
    settings.map(s => (s.kcql.getSource, CoapWriter(s))).map({ case (k, v) => writers.put(k, v) })
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    records.map(r => writers(r.topic()).write(List(r)))
    val seq = records.toVector
    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = {
    writers.foreach({ case (t, w) =>
      logger.info(s"Shutting down writer for $t")
      w.stop()
    })
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  override def version: String = manifest.version()

}

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

package com.datamountaineer.streamreactor.connect.jms.source

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import javax.jms.Message

import com.datamountaineer.streamreactor.connect.jms.config.{JMSConfig, JMSConfigConstants, JMSSettings}
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Created by andrew@datamountaineer.com on 10/03/2017. 
  * stream-reactor
  */
class JMSSourceTask extends SourceTask with StrictLogging {
  var reader: JMSReader = _
  val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val pollingTimeout: AtomicLong = new AtomicLong(0L)
  private var ackMessage: Option[Message] = None
  private val recordsToCommit = new ConcurrentHashMap[SourceRecord, SourceRecord]()
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/jms-source-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    JMSConfig.config.parse(props)
    val config = new JMSConfig(props)
    val settings = JMSSettings(config, sink = false)
    reader = JMSReader(settings)
    enableProgress = config.getBoolean(JMSConfigConstants.PROGRESS_COUNTER_ENABLED)
    pollingTimeout.set(settings.pollingTimeout)
  }

  override def stop(): Unit = {
    logger.info("Stopping JMS readers")

    synchronized {
      this.notifyAll()
    }

    reader.stop match {
      case Failure(t) => logger.error(s"Error encountered while stopping JMS Source Task. $t")
      case Success(_) => logger.info("Successfully stopped JMS Source Task.")
    }
  }

  override def poll(): util.List[SourceRecord] = {
    var records: mutable.Seq[SourceRecord] = mutable.Seq.empty[SourceRecord]
    var messages: mutable.Seq[Message] = mutable.Seq.empty[Message]

    try {
      val polled = reader.poll()

      if(polled.isEmpty) {
        synchronized {
          this.wait(pollingTimeout.get())
        }
      } else {
        records = collection.mutable.Seq(polled.map({ case (_, record) => record }).toSeq: _*)
        messages = collection.mutable.Seq(polled.map({ case (message, _) => message }).toSeq: _*)
      }
    } finally {
      if (messages.size > 0) {
        ackMessage = messages.headOption
        val polledRecordsToCommit = records.zip(records).toMap.asJava
        recordsToCommit.putAll(polledRecordsToCommit)
      }
    }

    if (enableProgress) {
      progressCounter.update(records.toVector)
    }

    records
  }

  override def commitRecord(record: SourceRecord): Unit = {
    recordsToCommit.remove(record)

    if (recordsToCommit.isEmpty) {
      ackMessage.foreach(_.acknowledge())
      ackMessage = None
    }
  }

  override def version: String = manifest.version()
}
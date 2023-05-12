/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.jms.source

import com.datamountaineer.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.datamountaineer.streamreactor.common.utils.ProgressCounter
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfig
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfigConstants
import com.datamountaineer.streamreactor.connect.jms.config.JMSSettings
import com.datamountaineer.streamreactor.connect.jms.source.readers.JMSReader
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiConsumer
import javax.jms.Message
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 10/03/2017.
  * stream-reactor
  */
class JMSSourceTask extends SourceTask with StrictLogging {
  private var reader: JMSReader = _
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean    = false
  private val pollingTimeout: AtomicLong = new AtomicLong(0L)
  private val recordsToCommit = new ConcurrentHashMap[SourceRecord, MessageAndTimestamp]()
  private val manifest        = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private val EmptyRecords    = Collections.emptyList[SourceRecord]()
  private var lastEvictedTimestamp: FiniteDuration = FiniteDuration(System.currentTimeMillis(), MILLISECONDS)
  private var evictInterval:        Int            = 0
  private var evictThreshold:       Int            = 0

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/jms-source-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    JMSConfig.config.parse(conf)
    val config   = new JMSConfig(conf)
    val settings = JMSSettings(config, sink = false)
    reader         = JMSReader(settings)
    enableProgress = config.getBoolean(JMSConfigConstants.PROGRESS_COUNTER_ENABLED)
    pollingTimeout.set(settings.pollingTimeout)
    evictInterval  = settings.evictInterval
    evictThreshold = settings.evictThreshold
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
    val polled = reader.poll()
    if (polled.isEmpty) {
      synchronized {
        this.wait(pollingTimeout.get())
      }
      if (enableProgress) {
        progressCounter.update(Vector.empty)
      }
      EmptyRecords
    } else {
      val timestamp = System.currentTimeMillis()
      val records = polled.map {
        case (msg, record) =>
          recordsToCommit.put(record, MessageAndTimestamp(msg, FiniteDuration(timestamp, MILLISECONDS)))
          record
      }
      if (enableProgress) {
        progressCounter.update(records)
      }
      records.asJava
    }
  }

  private def evictUncommittedMessages(): Unit = {
    val current = FiniteDuration(System.currentTimeMillis(), MILLISECONDS)
    if ((current - lastEvictedTimestamp).toMinutes > evictInterval) {
      recordsToCommit.forEach(
        new BiConsumer[SourceRecord, MessageAndTimestamp] {
          override def accept(t: SourceRecord, u: MessageAndTimestamp): Unit = evictIfApplicable(t, u, current)
        },
      )
    }
    lastEvictedTimestamp = current
  }

  private def evictIfApplicable(record: SourceRecord, msg: MessageAndTimestamp, now: FiniteDuration): Unit =
    if ((now - msg.timestamp).toMinutes > evictThreshold) {
      val _ = recordsToCommit.remove(record)
    }

  override def commitRecord(record: SourceRecord): Unit = {
    Option(recordsToCommit.remove(record)).foreach {
      case MessageAndTimestamp(msg, _) =>
        Try(msg.acknowledge())
    }
    evictUncommittedMessages()
  }

  override def version: String = manifest.version()
}

case class MessageAndTimestamp(msg: Message, timestamp: FiniteDuration)

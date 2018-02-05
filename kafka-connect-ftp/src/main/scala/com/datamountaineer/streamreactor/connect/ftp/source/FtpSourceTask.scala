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

package com.datamountaineer.streamreactor.connect.ftp.source

import java.time.Duration
import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.kafka.connect.storage.OffsetStorageReader

import scala.collection.JavaConverters._
import scala.collection.immutable.Stream.Empty
import scala.util.{Failure, Success}

// holds functions that translate a file meta+body into a source record


// bridges between the Connect reality and the (agnostic) FtpMonitor reality.
// logic could have been in FtpSourceTask directly, but FtpSourceTask's imperative nature made things a bit ugly,
// from a Scala perspective.
class FtpSourcePoller(cfg: FtpSourceConfig, offsetStorage: OffsetStorageReader) extends StrictLogging {
  val fileConverter = FileConverter(cfg.fileConverter, cfg.originalsStrings(), offsetStorage)

  val monitor2topic = cfg.ftpMonitorConfigs()
    .map(monitorCfg => (MonitoredPath(monitorCfg.path, monitorCfg.tail), monitorCfg.topic)).toMap

  val pollDuration = Duration.parse(cfg.getString(FtpSourceConfig.RefreshRate))
  val maxBackoff = Duration.parse(cfg.getString(FtpSourceConfig.MaxBackoff))

  var backoff = new ExponentialBackOff(pollDuration, maxBackoff)
  var buffer : Stream[SourceRecord] = Empty

  val ftpMonitor = {
    val (host,optPort) = cfg.address
    new FtpMonitor(
    FtpMonitorSettings(
      host,
      optPort,
      cfg.getString(FtpSourceConfig.User),
      cfg.getPassword(FtpSourceConfig.Password).value,
      Some(Duration.parse(cfg.getString(FtpSourceConfig.FileMaxAge))),
      monitor2topic.keys.toSeq,
      cfg.timeoutMs(),
      cfg.getProtocol,
      cfg.getString(FtpSourceConfig.fileFilter)
    ),
      fileConverter
    )
  }

  def poll(): Stream[SourceRecord] = {
    val stream = if (buffer.isEmpty) fetchRecords() else buffer
    val (head, tail) = stream.splitAt(cfg.maxPollRecords)
    buffer = tail
    head
  }

  def fetchRecords(): Stream[SourceRecord] = {
    if (backoff.passed) {
      logger.info("poll")
      ftpMonitor.poll() match {
        case Success(fileChanges) =>
          backoff = backoff.nextSuccess
          fileChanges.flatMap({ case (meta, body, w) =>
            logger.info(s"got some fileChanges: ${meta.attribs.path}")
            fileConverter.convert(monitor2topic(w), meta, body)
          })
        case Failure(err) =>
          logger.warn(s"ftp monitor failed: $err", err)
          backoff = backoff.nextFailure
          logger.info(s"let's backoff ${backoff.remaining}")
          Empty
      }
    } else {
      Thread.sleep(1000)
      Empty
    }
  }
}

class FtpSourceTask extends SourceTask with StrictLogging {
  var poller: Option[FtpSourcePoller] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def stop(): Unit = {
    logger.info("stop")
    poller = None
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("start")
    logger.info(manifest.printManifest())
    val sourceConfig = new FtpSourceConfig(props)
    sourceConfig.ftpMonitorConfigs.foreach(cfg => {
      val style = if (cfg.tail) "tail" else "updates"
      logger.info(s"config tells us to track the ${style} of files in `${cfg.path}` to topic `${cfg.topic}")
    })
    poller = Some(new FtpSourcePoller(sourceConfig, context.offsetStorageReader))
  }

  override def version(): String = manifest.version()

  override def poll(): util.List[SourceRecord] = poller match {
    case Some(poller) => poller.poll().asJava
    case None => throw new ConnectException("FtpSourceTask is not initialized but it is polled")
  }
}

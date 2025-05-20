/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.ftp.source

import io.lenses.streamreactor.connect.ftp.source.OpTimer.profile
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.source.SourceTask
import org.apache.kafka.connect.storage.OffsetStorageReader

import java.time.Duration
import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success

// holds functions that translate a file meta+body into a source record

// bridges between the Connect reality and the (agnostic) FtpMonitor reality.
// logic could have been in FtpSourceTask directly, but FtpSourceTask's imperative nature made things a bit ugly,
// from a Scala perspective.
class FtpSourcePoller(cfg: FtpSourceConfig, offsetStorage: OffsetStorageReader) extends StrictLogging {
  val fileConverter = FileConverter(cfg.fileConverter, cfg.originalsStrings(), offsetStorage)

  val monitor2topic = cfg.ftpMonitorConfigs()
    .map(monitorCfg => (MonitoredPath(monitorCfg.path, monitorCfg.mode), monitorCfg.topic)).toMap

  val pollDuration = Duration.parse(cfg.getString(FtpSourceConfig.RefreshRate))
  val maxBackoff   = Duration.parse(cfg.getString(FtpSourceConfig.MaxBackoff))

  var backoff = new ExponentialBackOff(pollDuration, maxBackoff)
  var buffer: LazyList[SourceRecord] = LazyList.empty

  val ftpMonitor = {
    val (host, optPort) = cfg.address()
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
        cfg.getString(FtpSourceConfig.fileFilter),
        cfg.getInt(FtpSourceConfig.MonitorSliceSize),
      ),
      fileConverter,
    )
  }

  def poll(): LazyList[SourceRecord] = {
    val stream: LazyList[SourceRecord] = if (buffer.isEmpty) fetchRecords() else buffer
    val (head, tail) = stream.splitAt(cfg.maxPollRecords)
    buffer = tail
    head
  }

  def fetchRecords(): LazyList[SourceRecord] =
    profile(
      "fetchRecords",
      if (backoff.passed) {
        logger.info("poll")
        ftpMonitor.poll() match {
          case Success(fileChanges) =>
            backoff = backoff.nextSuccess()
            val ret = fileChangesToRecords(fileChanges)
            ret
          case Failure(err) =>
            logger.warn(s"ftp monitor failed: $err", err)
            backoff = backoff.nextFailure()
            logger.info(s"let's backoff ${backoff.remaining}")
            LazyList.empty
        }
      } else {
        Thread.sleep(1000)
        LazyList.empty
      },
    )

  def fileChangesToRecords(fileChanges: LazyList[(FileMetaData, FileBody, MonitoredPath)]): LazyList[SourceRecord] =
    fileChanges.flatMap(
      {
        case (meta, body, w) =>
          logger.info(s"got some fileChanges: ${meta.attribs.path}, offset = ${meta.offset}")
          fileConverter.convert(monitor2topic(w), meta, body)
      },
    )
}

class FtpSourceTask extends SourceTask with StrictLogging with JarManifestProvided {
  var poller: Option[FtpSourcePoller] = None

  override def stop(): Unit = {
    logger.info("stop")
    poller = None
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("start")
    logger.info(manifest.buildManifestString())

    val conf = if (context.configs().isEmpty) props else context.configs()

    val sourceConfig = new FtpSourceConfig(conf)

    sourceConfig.ftpMonitorConfigs().foreach { cfg =>
      logger.info(s"config tells us to track the ${cfg.mode.toString} of files in `${cfg.path}` to topic `${cfg.topic}")
    }
    poller = Some(new FtpSourcePoller(sourceConfig, context.offsetStorageReader))
  }

  override def poll(): util.List[SourceRecord] = poller match {
    case Some(poller) => poller.poll().asJava
    case None         => throw new ConnectException("FtpSourceTask is not initialized but it is polled")
  }
}

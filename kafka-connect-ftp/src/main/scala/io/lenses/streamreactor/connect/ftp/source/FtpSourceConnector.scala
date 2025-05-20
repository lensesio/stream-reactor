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

import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import java.util
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Try

class FtpSourceConnector extends SourceConnector with StrictLogging with JarManifestProvided {
  private var configProps: Option[util.Map[String, String]] = None

  override def taskClass(): Class[_ <: Task] = classOf[FtpSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    configProps match {
      case Some(props) => (1 to maxTasks).map(_ => props).toList.asJava
      case None        => throw new ConnectException("cannot provide taskConfigs without being initialised")
    }
  }

  override def stop(): Unit =
    logger.info("stop")

  override def start(props: util.Map[String, String]): Unit = {

    printAsciiHeader(manifest, "/ftp-source-ascii.txt")

    logger.info(s"start FtpSourceConnector")

    configProps = Some(props)
    Try(new FtpSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start due to configuration error: " + f.getMessage, f)
      case _          =>
    }
  }

  override def config() = FtpSourceConfig.definition
}

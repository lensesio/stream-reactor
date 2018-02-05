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

import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

class FtpSourceConnector extends SourceConnector with StrictLogging {
  private var configProps : Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def taskClass(): Class[_ <: Task] = classOf[FtpSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    configProps match {
      case Some(props) => (1 to maxTasks).map(_ => props).toList.asJava
      case None => throw new ConnectException("cannot provide taskConfigs without being initialised")
    }
  }

  override def stop(): Unit = {
    logger.info("stop")
  }

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/ftp-source-ascii.txt")).mkString + s" v $version")
    logger.info(s"start FtpSourceConnector")

    configProps = Some(props)
    Try(new FtpSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start due to configuration error: " + f.getMessage, f)
      case _ =>
    }
  }

  override def version(): String = manifest.version()

  override def config() = FtpSourceConfig.definition
}

package com.datamountaineer.streamreactor.connect.ftp

import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

class FtpSourceConnector extends SourceConnector with StrictLogging {
  private var configProps : Option[util.Map[String, String]] = None

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
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/ftp-source-ascii.txt")).mkString)
    logger.info(s"start FtpSourceConnector ${GitRepositoryState.summary}")

    configProps = Some(props)
    Try(new FtpSourceConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start due to configuration error: " + f.getMessage, f)
      case _ =>
    }
  }

  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config() = FtpSourceConfig.definition
}

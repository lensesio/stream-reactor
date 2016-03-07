package com.datamountaineer.streamreactor.connect.kudu

import java.util
import com.datamountaineer.streamreactor.connect.Logging
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import scala.util.{Failure, Try}
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */
class KuduSinkConnector extends SinkConnector with Logging {
  private var configProps : util.Map[String, String] = null

  /**
    * States which SinkTask class to use
    * */
  override def taskClass(): Class[_ <: Task] = classOf[KuduSinkTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    * */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    log.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(c => configProps).toList.asJava
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    * */
  override def start(props: util.Map[String, String]): Unit = {
    log.info(s"Starting Elastic sink task with ${props.toString}.")
    configProps = props
    Try(new KuduSinkConfig(props)) match {
      case Failure(f) => throw new ConnectException("Couldn't start Elastic sink due to configuration error.", f)
      case _ =>
    }
  }

  override def stop(): Unit = {}
  override def version(): String = ""
}


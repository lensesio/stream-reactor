package com.datamountaineer.streamreactor.connect

/**
  * Created by andrew on 22/01/16.
  */

import java.util
import java.util.{List, Map}

import org.apache.kafka.connect.connector.{Connector, Task}

import scala.collection.JavaConverters._

class CassandraSinkConnector extends Connector {
  //???
  var configProps : Map[String, String] = null

  override def taskClass(): Class[_ <: Task] = ???

  override def taskConfigs(maxTasks: Int): List[Map[String, String]] = (1 to maxTasks).map(c => configProps).toList.asJava

  override def stop(): Unit = ???

  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
  }

  //fix
  override def version(): String = "unknown"
}

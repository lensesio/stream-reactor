package com.datamountaineer.streamreactor.connect.coap.source

import java.util

import com.datamountaineer.streamreactor.connect.coap.configs.CoapSourceConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
class CoapSourceConnector extends SourceConnector {
  private var configProps: util.Map[String, String] = _
  private val configDef = CoapSourceConfig.config

  override def taskClass(): Class[_ <: Task] = classOf[CoapSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val raw = configProps.asScala.get(CoapSourceConfig.COAP_KCQL)
    require(raw != null && !raw.isEmpty,  s"No ${CoapSourceConfig.COAP_KCQL} provided!")

    //sql1, sql2
    val kcqls = raw.get.split(";")
    val groups = ConnectorUtils.groupPartitions(kcqls.toList, maxTasks).asScala

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(g => g.isEmpty)
      .map(g => {
        val taskConfigs = new java.util.HashMap[String,String]
        taskConfigs.putAll(configProps)
        taskConfigs.put(CoapSourceConfig.COAP_KCQL, g.mkString(";")) //overwrite
        taskConfigs.toMap.asJava
      })
  }

  override def config(): ConfigDef = configDef

  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
  }

  override def stop(): Unit = {}

  override def version(): String = ???
}

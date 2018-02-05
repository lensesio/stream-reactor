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

package com.datamountaineer.streamreactor.connect.coap.source

import java.util

import com.datamountaineer.streamreactor.connect.coap.configs.{CoapConstants, CoapSourceConfig}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/12/2016. 
  * stream-reactor
  */
class CoapSourceConnector extends SourceConnector {
  private var configProps: util.Map[String, String] = _
  private val configDef = CoapSourceConfig.config
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def taskClass(): Class[_ <: Task] = classOf[CoapSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val raw = configProps.get(CoapConstants.COAP_KCQL)
    require(raw != null && !raw.isEmpty,  s"No ${CoapConstants.COAP_KCQL} provided!")

    //sql1, sql2
    val kcqls = raw.split(";")
    val groups = ConnectorUtils.groupPartitions(kcqls.toList, maxTasks)

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(g => g.isEmpty)
      .map(g => {
        val taskConfigs = new java.util.HashMap[String,String]
        taskConfigs.putAll(configProps)
        taskConfigs.put(CoapConstants.COAP_KCQL, g.mkString(";")) //overwrite
        taskConfigs.toMap.asJava
      })
  }

  override def config(): ConfigDef = configDef

  override def start(props: util.Map[String, String]): Unit = configProps = props

  override def stop(): Unit = {}

  override def version(): String = manifest.version()
}

/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.jms.source

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.jms.config.JMSConfig
import io.lenses.streamreactor.connect.jms.config.JMSConfigConstants
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
  * Created by andrew@datamountaineer.com on 10/03/2017.
  * stream-reactor
  */
class JMSSourceConnector extends SourceConnector with StrictLogging with JarManifestProvided {
  private var configProps: Map[String, String] = _
  private val configDef = JMSConfig.config

  override def taskClass(): Class[_ <: Task] = classOf[JMSSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val config    = new JMSConfig(configProps)
    val scaleType = config.getString(JMSConfigConstants.TASK_PARALLELIZATION_TYPE).toLowerCase()
    if (scaleType == JMSConfigConstants.TASK_PARALLELIZATION_TYPE_DEFAULT) {
      kcqlTaskScaling(maxTasks)
    } else defaultTaskScaling(maxTasks)
  }

  override def config(): ConfigDef = configDef

  override def start(props: util.Map[String, String]): Unit = {
    val scalaProps = props.asScala.toMap
    val config     = new JMSConfig(scalaProps)
    configProps = config.props
  }

  override def stop(): Unit = {}

  private def kcqlTaskScaling(maxTasks: Int): util.List[util.Map[String, String]] = {
    val raw = getRawKcqlString

    //sql1, sql2
    val kcqls: Array[String] = raw.map(e => e.split(";")).getOrElse(Array.empty)
    val groups = ConnectorUtils.groupPartitions(kcqls.toList.asJava, maxTasks).asScala

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(_.isEmpty)
      .map { g: util.List[String] =>
        (configProps + (JMSConfigConstants.KCQL -> g.asScala.mkString(";"))).asJava
      }
  }.asJava

  private def defaultTaskScaling(maxTasks: Int): util.List[util.Map[String, String]] = {
    getRawKcqlString
    (1 to maxTasks).map(_ => configProps.asJava).toList.asJava
  }

  private def getRawKcqlString: Option[String] = {
    val raw: Option[String] = configProps.get(JMSConfigConstants.KCQL)
    require(raw.nonEmpty, s"No ${JMSConfigConstants.KCQL} provided!")
    raw
  }

}

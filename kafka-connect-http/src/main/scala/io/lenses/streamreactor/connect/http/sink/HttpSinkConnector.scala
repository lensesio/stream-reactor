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
package io.lenses.streamreactor.connect.http.sink

import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfigDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class HttpSinkConnector extends SinkConnector with LazyLogging {

  private val manifest =  new JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var props:         Option[Map[String, String]] = Option.empty
  private var maybeSinkName: Option[String]              = Option.empty

  private def sinkName = maybeSinkName.getOrElse("Lenses.io HTTP Sink")
  override def version(): String = manifest.getVersion()

  override def taskClass(): Class[_ <: Task] = classOf[HttpSinkTask]

  override def config(): ConfigDef = HttpSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    val propsAsScala = props.asScala.toMap
    this.props         = propsAsScala.some
    this.maybeSinkName = propsAsScala.get("name")

    logger.info(s"[$sinkName] Creating HTTP sink connector")
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"[$sinkName] Creating $maxTasks tasks config")
    List.fill(maxTasks) {
      props.map(_.asJava).getOrElse(Map.empty[String, String].asJava)
    }.asJava
  }
}

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
package io.lenses.streamreactor.connect.cloud.common.guard

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

/**
  * Builds Kafka Admin/Producer client properties for the guard components.
*/
object KafkaGuardClientProps {

  private val GenericPassThroughPrefix   = "guard.client."
  private val AdminPassThroughPrefix     = "guard.admin.client."
  private val ProducerPassThroughPrefix  = "guard.producer.client."

  private def copyAllowed(ctxProps: Map[String, String], allowed: java.util.Set[String], out: Properties): Unit = {
    ctxProps.iterator.foreach {
      case (k, v) if allowed.contains(k) => out.put(k, v)
      case _                              => ()
    }
  }

  private def applyPassThrough(ctxProps: Map[String, String], prefix: String, out: Properties): Unit = {
    if (prefix.nonEmpty) {
      ctxProps.iterator.foreach {
        case (k, v) if k.startsWith(prefix) && k.length > prefix.length =>
          out.put(k.substring(prefix.length), v)
        case _ => ()
      }
    }
  }

  def buildAdminProps(bootstrapServers: String, ctxProps: Map[String, String]): Properties = {
    val props = new Properties()
    val adminAllowed = AdminClientConfig.configDef().names()

    copyAllowed(ctxProps, adminAllowed, props)
    applyPassThrough(ctxProps, GenericPassThroughPrefix, props)
    applyPassThrough(ctxProps, AdminPassThroughPrefix, props)
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props
  }

  def buildProducerProps(bootstrapServers: String, ctxProps: Map[String, String]): Properties = {
    val props = new Properties()
    val producerAllowed = ProducerConfig.configDef().names()

    copyAllowed(ctxProps, producerAllowed, props)
    applyPassThrough(ctxProps, GenericPassThroughPrefix, props)
    applyPassThrough(ctxProps, ProducerPassThroughPrefix, props)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props
  }
}

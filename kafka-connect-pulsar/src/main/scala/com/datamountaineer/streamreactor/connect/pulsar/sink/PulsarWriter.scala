/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.pulsar.sink

import com.datamountaineer.streamreactor.common.errors.ErrorHandler
import com.datamountaineer.streamreactor.connect.pulsar.ProducerConfigFactory
import com.datamountaineer.streamreactor.connect.pulsar.config.PulsarSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.pulsar.client.api._
import org.apache.pulsar.client.impl.auth.AuthenticationTls

import scala.annotation.nowarn
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

object PulsarWriter {

  @nowarn
  def apply(name: String, settings: PulsarSinkSettings): PulsarWriter = {

    lazy val client = PulsarClient.builder()

    settings.sslCACertFile.foreach { f =>
      val authParams = settings.sslCertFile.map(f => ("tlsCertFile", f)).toMap ++ settings.sslCertKeyFile.map(f =>
        ("tlsKeyFile", f),
      ).toMap

      client.enableTls(true)
        .tlsTrustCertsFilePath(f)
        .authentication(classOf[AuthenticationTls].getName, authParams.asJava)
    }

    val clientBuilt           = client.serviceUrl(settings.connection).build()
    val producerConfigFactory = new ProducerConfigFactory(clientBuilt)
    new PulsarWriter(
      () => clientBuilt.close(),
      () => producerConfigFactory(name, settings.kcql),
      settings,
    )
  }
}

case class PulsarWriter(
  fnCloseClient:     () => Unit,
  fnCreateProducers: () => Map[String, ProducerBuilder[Array[Byte]]],
  settings:          PulsarSinkSettings,
) extends StrictLogging
    with ErrorHandler {
  //initialize error tracker
  initialize(settings.maxRetries, settings.errorPolicy)

  private val producersMap = mutable.Map.empty[String, Producer[Array[Byte]]]
  private val msgFactory   = PulsarMessageTemplateBuilder(settings)
  private val configs      = fnCreateProducers()

  def write(records: Iterable[SinkRecord]) = {
    val messages = msgFactory.create(records)

    val t = Try {
      messages.foreach {
        messageTemplate: MessageTemplate =>
          val producer =
            producersMap.getOrElseUpdate(messageTemplate.pulsarTopic, configs(messageTemplate.pulsarTopic).create())
          messageTemplate.toMessage(producer).send()
      }
    }

    handleTry(t)
  }

  def flush(): Unit = {}

  def close(): Unit = {
    logger.info("Closing client")
    producersMap.foreach({ case (_, producer) => producer.close() })
    fnCloseClient()
  }
}

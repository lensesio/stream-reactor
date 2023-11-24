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
package io.lenses.streamreactor.connect.pulsar

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.pulsar.config.KcqlMessageRouting
import com.typesafe.scalalogging.StrictLogging
import org.apache.pulsar.client.api.CompressionType
import org.apache.pulsar.client.api.ProducerBuilder
import org.apache.pulsar.client.api.PulsarClient

import java.util.concurrent.TimeUnit

/**
  * Created by andrew@datamountaineer.com on 22/01/2018.
  * stream-reactor
  */
class ProducerConfigFactory(pulsarClient: PulsarClient) extends StrictLogging {
  def apply(name: String, kcqls: Set[Kcql]): Map[String, ProducerBuilder[Array[Byte]]] =
    kcqls.map { kcql =>
      val conf = pulsarClient.newProducer()

      // set batching
      if (kcql.getBatchSize > 0) {
        conf.enableBatching(true)
        conf.batchingMaxMessages(kcql.getBatchSize)

        if (kcql.getWithDelay > 0) {
          conf.batchingMaxPublishDelay(kcql.getWithDelay.toLong, TimeUnit.MILLISECONDS)
        }
      }

      // set compression type
      if (kcql.getWithCompression != null) {

        val compressionType = kcql.getWithCompression match {
          case io.lenses.kcql.CompressionType.LZ4  => CompressionType.LZ4
          case io.lenses.kcql.CompressionType.ZLIB => CompressionType.ZLIB
          case _ =>
            logger.warn(s"Unknown supported compression type ${kcql.getWithCompression.toString}. Defaulting to LZ4")
            CompressionType.LZ4
        }

        conf.compressionType(compressionType)
      }

      // set routing mode
      conf.messageRoutingMode(KcqlMessageRouting(kcql))
      conf.producerName(name)

      (kcql.getTarget, conf)
    }.toMap

}

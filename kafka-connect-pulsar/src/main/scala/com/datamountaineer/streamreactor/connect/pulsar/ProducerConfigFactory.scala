package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.pulsar.config.KcqlMessageRouting
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
          case com.datamountaineer.kcql.CompressionType.LZ4  => CompressionType.LZ4
          case com.datamountaineer.kcql.CompressionType.ZLIB => CompressionType.ZLIB
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

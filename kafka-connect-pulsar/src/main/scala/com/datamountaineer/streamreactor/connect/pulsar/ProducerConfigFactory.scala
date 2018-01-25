package com.datamountaineer.streamreactor.connect.pulsar

import java.util.concurrent.TimeUnit

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.pulsar.client.api.ProducerConfiguration.MessageRoutingMode
import org.apache.pulsar.client.api.{CompressionType, ProducerConfiguration}

/**
  * Created by andrew@datamountaineer.com on 22/01/2018. 
  * stream-reactor
  */
object ProducerConfigFactory extends StrictLogging {
  def apply(name: String, kcqls : Set[Kcql]): Map[String, ProducerConfiguration] = {


    kcqls.map(kcql => {
      val conf = new ProducerConfiguration()

      // set batching
      if (kcql.getBatchSize > 0) {
        conf.setBatchingEnabled(true)
        conf.setBatchingMaxMessages(kcql.getBatchSize)

        if (kcql.getWithDelay > 0) {
          conf.setBatchingMaxPublishDelay(kcql.getWithDelay, TimeUnit.MILLISECONDS)
        }
      }

      // set compression type
      if (kcql.getWithCompression != null) {

        val compressionType = kcql.getWithCompression match {
          case com.datamountaineer.kcql.CompressionType.LZ4 => CompressionType.LZ4
          case com.datamountaineer.kcql.CompressionType.ZLIB => CompressionType.ZLIB
          case _ =>
            logger.warn(s"Unknown supported compression type ${kcql.getWithCompression.toString}. Defaulting to LZ4")
            CompressionType.LZ4
        }

        conf.setCompressionType(compressionType)
      }

      // set routing mode
      conf.setMessageRoutingMode(getMessageRouting(kcql))
      conf.setProducerName(name)

      (kcql.getTarget, conf)
    }).toMap
  }

  def getMessageRouting(kcql: Kcql): MessageRoutingMode = {
    // set routing mode
    // match on strings as not enums and Puslar are camelcase
    if (kcql.getWithPartitioner != null) {
      kcql.getWithPartitioner.trim.toUpperCase match {
        case "SINGLEPARTITION" =>
          MessageRoutingMode.SinglePartition

        case "ROUNDROBINPARTITION" =>
          MessageRoutingMode.RoundRobinPartition

        case "CUSTOMPARTITION" =>
          MessageRoutingMode.CustomPartition

        case _ =>
          logger.error(s"Unknown message routing mode ${kcql.getWithType}. Defaulting to SinglePartition")
          MessageRoutingMode.SinglePartition
      }

    } else {
      logger.info(s"Defaulting to SinglePartition message routing mode")
      MessageRoutingMode.SinglePartition
    }
  }
}

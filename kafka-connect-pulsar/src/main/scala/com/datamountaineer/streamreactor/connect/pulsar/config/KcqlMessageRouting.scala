package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import org.apache.pulsar.client.api.MessageRoutingMode

object KcqlMessageRouting extends LazyLogging {

  def apply(kcql: Kcql): MessageRoutingMode = {
    // set routing mode
    // match on strings as not enums and Pulsar are camelcase
    if (kcql.getWithPartitioner != null) {
      kcql.getWithPartitioner.trim.toUpperCase match {
        case "SINGLEPARTITION" =>
          MessageRoutingMode.SinglePartition

        case "ROUNDROBINPARTITION" =>
          MessageRoutingMode.RoundRobinPartition

        case "CUSTOMPARTITION" =>
          MessageRoutingMode.CustomPartition

        case _ =>
          logger.error(s"Unknown message routing mode '${kcql.getWithPartitioner}'. Defaulting to SinglePartition")
          MessageRoutingMode.SinglePartition
      }

    } else {
      logger.info(s"Defaulting to SinglePartition message routing mode")
      MessageRoutingMode.SinglePartition
    }
  }
}

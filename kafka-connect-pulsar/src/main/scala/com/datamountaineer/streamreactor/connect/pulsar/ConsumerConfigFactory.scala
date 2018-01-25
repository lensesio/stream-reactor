package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.pulsar.client.api.{ConsumerConfiguration, SubscriptionType}


/**
  * Created by andrew@datamountaineer.com on 22/01/2018. 
  * stream-reactor
  */
object ConsumerConfigFactory extends StrictLogging {

  def apply(name: String, kcqls: Set[Kcql]): Map[String, ConsumerConfiguration] = {
    kcqls.map(kcql => {
      val config = new ConsumerConfiguration

      if (kcql.getBatchSize != null) {
        config.setReceiverQueueSize(kcql.getBatchSize())
      }

      config.setSubscriptionType(getSubscriptionType(kcql))
      config.setConsumerName(name)
      (kcql.getSource, config)
    }).toMap
  }

  def getSubscriptionType(kcql: Kcql): SubscriptionType = {

    if (kcql.getWithSubscription() != null) {
      kcql.getWithSubscription.toUpperCase.trim match {
        case "EXCLUSIVE" =>
          SubscriptionType.Exclusive

        case "FAILOVER" =>
          SubscriptionType.Failover

        case "SHARED" =>
          SubscriptionType.Shared

        case _ =>
          logger.error(s"Unsupported subscription type ${kcql.getWithType} set in WITHTYPE. Defaulting to Failover")
          SubscriptionType.Failover
      }
    } else {
      logger.info("Defaulting to failover subscription type")
      SubscriptionType.Failover
    }
  }
}

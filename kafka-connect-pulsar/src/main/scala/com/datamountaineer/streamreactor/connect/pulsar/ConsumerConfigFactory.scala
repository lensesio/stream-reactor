package com.datamountaineer.streamreactor.connect.pulsar

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.pulsar.config.KcqlSubscriptionType
import com.typesafe.scalalogging.StrictLogging
import org.apache.pulsar.client.api.{ConsumerBuilder, PulsarClient}


/**
  * Created by andrew@datamountaineer.com on 22/01/2018. 
  * stream-reactor
  */
class ConsumerConfigFactory(pulsarClient: PulsarClient) extends StrictLogging {

  def apply(name: String, kcqls: Set[Kcql]): Map[String, ConsumerBuilder[Array[Byte]]] = {
    kcqls.map(kcql => {
      val config = pulsarClient.newConsumer()

      if (kcql.getBatchSize > 0) config.receiverQueueSize(kcql.getBatchSize)
      config.subscriptionType(KcqlSubscriptionType(kcql))
      config.consumerName(name)
      (kcql.getSource, config)
    }).toMap
  }

}

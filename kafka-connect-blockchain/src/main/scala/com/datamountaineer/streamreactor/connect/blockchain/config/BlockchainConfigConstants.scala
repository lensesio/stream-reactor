

package com.datamountaineer.streamreactor.connect.blockchain.config

/**
  * Created by carolinaloureiro on 10/04/2017.
  * stream-reactor
  */


/**
  * Holds the constants used in the config.
  **/

object BlockchainConfigConstants {
  val CONNECTION_URL = "connect.blockchain.source.url"
  val CONNECTION_URL_DOC = "The websocket connection."
  val CONNECTION_URL_DEFAULT = "wss://ws.blockchain.info/inv"

  val ADDRESS_SUBSCRIPTION = "connect.blockchain.source.subscription.addresses"
  val ADDRESS_SUBSCRIPTION_DOC = "Comma separated list of addresses to receive transactions updates for"

  val KAFKA_TOPIC = "connect.blockchain.source.kafka.topic"
  val KAFKA_TOPIC_DOC = "Specifies the kafka topic to sent the records to."

}

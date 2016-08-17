package com.datamountaineer.streamreactor.socketstreamer

import java.util.Properties

import com.datamountaineer.streamreactor.socketstreamer.flows.KafkaConstants
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.utils.VerifiableProperties

object KafkaAvroDecoderFn {
  def apply(config: SocketStreamerConfig) = {
    val props = new Properties()
    props.put(KafkaConstants.ZOOKEEPER_KEY, config.zookeeper)
    props.put(KafkaConstants.SCHEMA_REGISTRY_URL, config.schemaRegistryUrl)
    val vProps = new VerifiableProperties(props)
    new KafkaAvroDecoder(vProps)
  }
}

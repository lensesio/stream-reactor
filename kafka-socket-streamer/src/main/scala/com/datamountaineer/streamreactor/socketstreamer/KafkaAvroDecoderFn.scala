/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.socketstreamer

import java.util.Properties

import com.datamountaineer.streamreactor.socketstreamer.flows.KafkaConstants
import io.confluent.kafka.serializers.KafkaAvroDecoder
import kafka.utils.VerifiableProperties

object KafkaAvroDecoderFn {
  def apply(config: SocketStreamerConfig): KafkaAvroDecoder = {
    val props = new Properties()
    props.put(KafkaConstants.ZOOKEEPER_KEY, config.zookeeper)
    props.put(KafkaConstants.SCHEMA_REGISTRY_URL, config.schemaRegistryUrl)
    val vProps = new VerifiableProperties(props)
    new KafkaAvroDecoder(vProps)
  }
}

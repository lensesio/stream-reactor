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
import io.lenses.streamreactor.connect.pulsar.config.KcqlSubscriptionType
import com.typesafe.scalalogging.StrictLogging
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.PulsarClient

/**
  * Created by andrew@datamountaineer.com on 22/01/2018.
  * stream-reactor
  */
class ConsumerConfigFactory(pulsarClient: PulsarClient) extends StrictLogging {

  def apply(name: String, kcqls: Set[Kcql]): Map[String, ConsumerBuilder[Array[Byte]]] =
    kcqls.map { kcql =>
      val config = pulsarClient.newConsumer()

      if (kcql.getBatchSize > 0) config.receiverQueueSize(kcql.getBatchSize)
      config.subscriptionType(KcqlSubscriptionType(kcql))
      config.consumerName(name)
      (kcql.getSource, config)
    }.toMap

}

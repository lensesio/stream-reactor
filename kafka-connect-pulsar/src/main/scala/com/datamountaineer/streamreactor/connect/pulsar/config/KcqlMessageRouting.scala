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
package io.lenses.streamreactor.connect.pulsar.config

import io.lenses.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import org.apache.pulsar.client.api.MessageRoutingMode

object KcqlMessageRouting extends LazyLogging {

  def apply(kcql: Kcql): MessageRoutingMode =
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

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
package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import enumeratum._
import org.apache.pulsar.client.api.{ SubscriptionType => PulsarSubscriptionType }

import scala.util.Try

sealed abstract class KcqlSubscriptionType(pulsarSubscriptionType: PulsarSubscriptionType) extends EnumEntry {
  def toPulsarSubscriptionType: PulsarSubscriptionType = pulsarSubscriptionType
}

object KcqlSubscriptionType extends Enum[KcqlSubscriptionType] with LazyLogging {

  case object Exclusive extends KcqlSubscriptionType(PulsarSubscriptionType.Exclusive)
  case object Failover  extends KcqlSubscriptionType(PulsarSubscriptionType.Failover)
  case object Shared    extends KcqlSubscriptionType(PulsarSubscriptionType.Shared)

  def apply(kcql: Kcql): PulsarSubscriptionType =
    KcqlSubscriptionType
      .withNameInsensitiveOption(Try(kcql.getWithSubscription.trim).getOrElse(""))
      .getOrElse {
        logMissingSubscriptionType(kcql)
        Failover
      }.toPulsarSubscriptionType

  private def logMissingSubscriptionType(kcql: Kcql) =
    if (kcql.getWithSubscription == null) {
      logger.info("Defaulting to failover subscription type")
    } else {
      logger.info(
        s"Unsupported subscription type '${kcql.getWithSubscription}' set in WITHTYPE. Defaulting to Failover",
      )
    }

  override def values: IndexedSeq[KcqlSubscriptionType] = findValues
}

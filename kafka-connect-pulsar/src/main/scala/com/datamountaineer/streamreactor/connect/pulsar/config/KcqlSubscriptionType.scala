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
      logger.info(s"Unsupported subscription type '${kcql.getWithSubscription}' set in WITHTYPE. Defaulting to Failover")
    }

  override def values: IndexedSeq[KcqlSubscriptionType] = findValues
}

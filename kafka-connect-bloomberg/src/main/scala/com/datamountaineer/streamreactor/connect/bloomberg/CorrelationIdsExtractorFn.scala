package com.datamountaineer.streamreactor.connect.bloomberg

import com.bloomberglp.blpapi.SubscriptionList
import scala.collection.JavaConverters._

object CorrelationIdsExtractorFn {
  /**
    * Given a list of Bloomberg subscriptions builds a list of all the correlation ids.
    *
    * @param subscriptions : A list of Bloomberg subscriptions
    * @return A string listing all the correlation ids associated with the input parameter
    */
  def apply(subscriptions: SubscriptionList) = {
    if (subscriptions != null)
      subscriptions.asScala.map(_.correlationID().value()).mkString(",")
    else ""
  }
}

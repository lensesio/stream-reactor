package com.datamountaineer.streamreactor.connect.bloomberg


/**
  * Holds the ticker and the fields to receive data for
  *
  * @param ticket The ticker/security identifier (i.e. '/ticker/GOOG US Equity'/'MSFT US Equity')
  * @param fields Sequence of fields to receive data for
  */
case class SubscriptionInfo(ticket: String, fields: Seq[String]) {
  override def toString = s"$ticket:${fields.mkString(",")}"
}

/**
  * From the configuration it will provide a sequence of SubscriptionInfo. The configuration template is
  * ticker1:FIELD1,FIELD2,..;ticker2:FIELD11,FIELD12,..;ticker3:FIELD31,FIELD31,...
  * All the allowed fields are defined by BloombergConstants.SubscriptionFields
  */
object SubscriptionInfoExtractFn {
  def apply(source: String): Seq[SubscriptionInfo] = {
    require(source != null && source.trim.nonEmpty, "Invalid subscription setting.The format is <Ticker:Field1,Field2[;Ticker2:field1,field2;...]")
    source.split(";").map { case subscription =>
      val index = subscription.indexOf(":")
      if (index < 0)
        throw new IllegalArgumentException("Invalid configuration. Missing \":\". The format is <Ticker:Field1,Field2[;Ticker2:field1,field2;...]")
      val ticker = subscription.substring(0, index).trim
      val fields = subscription.substring(index + 1).split(",").map(_.trim.toUpperCase).filterNot(_.isEmpty).toSet.toList
      require(fields.nonEmpty, s"You need to provide at least one field for $subscription")
      SubscriptionInfo(ticker, fields)
    }.toList
  }
}
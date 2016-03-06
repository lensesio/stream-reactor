package com.datamountaineer.streamreactor.connect.bloomberg

/**
  * Holds the values associated with an update event for a given ticker
  *
  * @param subscription : The ticker for which the data was received from Bloomberg
  * @param fields       : A map of field=value
  */
case class BloombergData(subscription: String, fields: java.util.Map[String, Any])

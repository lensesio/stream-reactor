package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import com.bloomberglp.blpapi.Element

/**
  * Holds the values associated with an update event for a given ticker
  *
  * @param subscription : The ticker for which the data was received from Bloomberg
  * @param fields       : A map of field=value
  */
private[bloomberg] case class BloombergData(subscription: String, fields: java.util.Map[String, Any])


private[bloomberg] object BloombergData {
  /**
    * Converts a Bloomberg Element instance to a BloombergData one
    *
    * @param ticker
    * @param element
    * @return
    */
  def apply(ticker: String, element: Element): BloombergData = {
    val fields = (0 until element.numValues())
      .map(element.getElement)
      .filter(f => !f.isNull)
      .foldLeft(new util.LinkedHashMap[String, Any]()) { case (map, f) =>
        val value = BloombergFieldValueFn(f)
        map.put(f.name().toString, value)
        map
      }

    BloombergData(ticker, fields)
  }
}

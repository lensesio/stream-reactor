package com.datamountaineer.streamreactor.connect.bloomberg

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, OffsetTime, ZoneOffset}

import com.bloomberglp.blpapi.{Schema, Datetime, Element}

import scala.collection.JavaConverters._

/**
  * Extracts the values held within the the bloomberg message.
  */
object BloombergFieldValueFn {
  lazy val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  /**
    * Returns the value contained by the element. Given its data type information(see Schema.Datatype)
    * returns the appropriate value
    * @param element: Instance of the bloomberg field update
    * @return The value encapsulated by the element
    */
  def apply(element: Element): Any = {
    element.datatype().intValue() match {
      case 6 /*Schema.Datatype.FLOAT64*/ => element.getValueAsFloat64()
      case 5 /*Schema.Datatype.FLOAT32*/ => element.getValueAsFloat32()
      case 0 /*Schema.Datatype.BOOL*/ => element.getValueAsBool()
      case 1 /*Schema.Datatype.CHAR*/ => element.getValueAsChar()
      case 7 /* Schema.Datatype.INT32*/ => element.getValueAsInt32()
      case 8 /*Schema.Datatype.INT64*/ => element.getValueAsInt64()
      case 9 /*Schema.Datatype.STRING*/ => element.getValueAsString()
      case 2 /*Schema.Datatype.DATE*/ => localDate(element.getValueAsDate())
      case 10 /*Schema.Datatype.TIME*/ => offsetDateTime(element.getValueAsDatetime)
      case 3 /*Schema.Datatype.DATETIME*/ =>
        val dt = element.getValueAsDatetime()
        if (dt.hasParts(Datetime.DATE)) {
          if (dt.hasParts(Datetime.TIME))
            offsetDateTime(dt)
          else
            localDate(dt)
        }
        offsetDateTime(dt)

      case 258 | 259 /*Schema.Datatype.SEQUENCE | Schema.Datatype.CHOICE*/ =>
        element.elementIterator().asScala.foldLeft(new java.util.HashMap[String, Any]) { case (map, element) =>
          map.put(element.name().toString, BloombergFieldValueFn(element))
          map
        } //needs to be a java map because of json serialization

      case _ =>
        if (element.isArray) {
          (0 to element.numValues()).map { i =>
            BloombergFieldValueFn(element.getValueAsElement(i))
          }.asJava
        }
        else element.toString
    }
  }

  def offsetDateTime(dt: Datetime): String = {
    val offsetSeconds = if (dt.hasParts(Datetime.TIME_ZONE_OFFSET)) dt.timezoneOffsetMinutes() * 60 else 0
    val offset = ZoneOffset.ofTotalSeconds(offsetSeconds)
    OffsetTime.of(dt.hour(), dt.minute(), dt.second(), dt.nanosecond(), offset).toString
  }

  def localDate(dt: Datetime): String = {
    LocalDate.of(dt.year(), dt.month(), dt.dayOfMonth()).format(datetimeFormatter)
  }

}

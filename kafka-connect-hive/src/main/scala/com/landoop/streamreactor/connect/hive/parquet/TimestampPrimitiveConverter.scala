package com.landoop.streamreactor.connect.hive.parquet

import jodd.datetime.{JDateTime, JulianDateStamp}
import org.apache.kafka.connect.data.Field
import org.apache.parquet.example.data.simple.NanoTime
import org.apache.parquet.io.api.{Binary, PrimitiveConverter}

// see https://issues.apache.org/jira/browse/HIVE-6394 and
// https://issues.apache.org/jira/browse/SPARK-10177 for compile ideas
class TimestampPrimitiveConverter(field: Field, builder: scala.collection.mutable.Map[String, Any]) extends PrimitiveConverter {

  private val nanosInDay = BigDecimal(60 * 60 * 24) * 1000 * 1000 * 1000
  private val offset = nanosInDay / 2

  override def addBinary(x: Binary): Unit = {
    val nano = NanoTime.fromBinary(x)
    val jdt = new JDateTime()
    val f = (BigDecimal(nano.getTimeOfDayNanos) - offset) / nanosInDay
    jdt.setJulianDate(new JulianDateStamp(nano.getJulianDay, f.doubleValue()))
    builder.put(field.name, jdt.convertToSqlTimestamp)
  }
}
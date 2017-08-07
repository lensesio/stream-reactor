package com.datamountaineer.streamreactor.connect.influx.writers

import java.time.Instant
import java.util.Date

import org.apache.kafka.common.config.ConfigException

import scala.util.Try

object TimestampValueCoerce {
  def apply(value: Any)(implicit fieldPath: Vector[String]): Long = {
    value match {
      case b: Byte => b.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case s: String => Try(Instant.parse(s).toEpochMilli).getOrElse(throw new IllegalArgumentException(s"$s is not a valid format for timestamp, expected 'yyyy-MM-DDTHH:mm:ss.SSSZ'"))
      case d: Date => d.toInstant.toEpochMilli
      case other => throw new ConfigException(s"Invalid value for field:${fieldPath.mkString(".")}.Value '$other' is not a valid field for the timestamp")
    }
  }
}

/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.influx.writers

import java.time.Instant
import java.util.Date

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
      /* Assume Unix timestamps in seconds with double precision, coerce to Long with milliseconds precision */
      case d: Double => (d * 1E3).toLong
      case other => throw new IllegalArgumentException(s"Invalid value for field:${fieldPath.mkString(".")}.Value '$other' is not a valid field for the timestamp")
    }
  }
}

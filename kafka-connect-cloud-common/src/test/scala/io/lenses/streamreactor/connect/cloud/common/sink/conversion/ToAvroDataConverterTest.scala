/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import io.lenses.streamreactor.connect.cloud.common.formats.writer.DateSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.TimeSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.TimestampSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.dateWithTimeFieldsOnly
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.daysSinceEpoch
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date

class ToAvroDataConverterTest extends AnyFunSuiteLike with Matchers {

  test("should convert date") {
    val date = Date.from(Instant.now().truncatedTo(ChronoUnit.DAYS))
    val daysSince: Long = daysSinceEpoch(date)
    val converted = ToAvroDataConverter.convertToGenericRecord(DateSinkData(date))
    checkValueAndSchema(converted, daysSince)
  }

  test("should convert time") {
    val asDate: Date = dateWithTimeFieldsOnly(12, 30, 45, 450)
    val converted = ToAvroDataConverter.convertToGenericRecord(TimeSinkData(asDate))
    checkValueAndSchema(converted, asDate.getTime)
  }

  test("should convert timestamp") {
    val date      = Date.from(Instant.now())
    val converted = ToAvroDataConverter.convertToGenericRecord(TimestampSinkData(date))
    checkValueAndSchema(converted, date.getTime)
  }

  private def checkValueAndSchema(converted: Any, expectedValue: Long): Any =
    converted match {
      case nonRecordContainer: Long =>
        nonRecordContainer should be(expectedValue)
      case _ => fail("not a non-record container")
    }

}

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
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.dateWithTimeFieldsOnly
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.daysSinceEpoch
import org.apache.kafka.connect.json.DecimalFormat
import org.apache.kafka.connect.json.JsonConverter
import org.apache.kafka.connect.json.JsonConverterConfig
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import scala.jdk.CollectionConverters.MapHasAsJava

class ToJsonDataConverterTest extends AnyFunSuiteLike with Matchers {

  private val ExampleTopic: Topic = Topic("myTopic")

  private val Converter = new JsonConverter()
  Converter.configure(
    Map("schemas.enable" -> "false", JsonConverterConfig.DECIMAL_FORMAT_CONFIG -> DecimalFormat.NUMERIC.name()).asJava,
    false,
  )

  test("should convert date") {
    val date = Date.from(Instant.now().truncatedTo(ChronoUnit.DAYS))
    val daysSince: Long = daysSinceEpoch(date)
    val converted = ToJsonDataConverter.convertMessageValueToByteArray(Converter, ExampleTopic, DateSinkData(date))
    checkValueAndSchema(converted, daysSince.toString)
  }

  test("should convert time") {
    val asDate: Date = dateWithTimeFieldsOnly(12, 30, 45, 450)
    val converted = ToJsonDataConverter.convertMessageValueToByteArray(Converter, ExampleTopic, TimeSinkData(asDate))
    checkValueAndSchema(converted, asDate.getTime.toString)
  }

  test("should convert timestamp") {
    val date      = Date.from(Instant.now())
    val converted = ToJsonDataConverter.convertMessageValueToByteArray(Converter, ExampleTopic, TimestampSinkData(date))
    checkValueAndSchema(converted, date.getTime.toString)
  }

  private def checkValueAndSchema(converted: Any, expectedValue: String): Any =
    converted match {
      case nonRecordContainer: Array[Byte] =>
        new String(nonRecordContainer) should be(expectedValue)
      case x => fail(s"not a byte array ($converted) but a ${x.getClass.getName}")
    }

}

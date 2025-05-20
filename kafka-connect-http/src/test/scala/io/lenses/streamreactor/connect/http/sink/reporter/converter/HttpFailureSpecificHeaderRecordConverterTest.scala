/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink.reporter.converter
import cyclops.control.Option
import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpFailureSpecificHeaderRecordConverter._
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpFailureConnectorSpecificRecordData
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.jdk.StreamConverters._

class HttpFailureSpecificHeaderRecordConverterTest extends AnyFunSuiteLike with Matchers with OptionValues {

  private val fnConvert: HttpFailureConnectorSpecificRecordData => Array[Header] =
    in => HttpFailureSpecificHeaderRecordConverter.apply(originalRecord = in).toScala(Array)

  private val errorMessage    = "Warp core breach"
  private val responseContent = "response-content"
  private val statusCode      = 500

  test("should convert HttpFailureConnectorSpecificRecordData to headers") {

    val recordData = HttpFailureConnectorSpecificRecordData(
      Option.of(statusCode),
      Option.of(responseContent),
      errorMessage,
    )

    val headers = fnConvert(recordData)

    headers should contain allOf (
      new RecordHeader(headerNameError, errorMessage.getBytes),
      new RecordHeader(headerNameResponseContent, responseContent.getBytes),
      new RecordHeader(headerNameStatusCode, String.valueOf(statusCode).getBytes)
    )
  }

  test("should handle empty response content and status code") {

    val recordData = HttpFailureConnectorSpecificRecordData(
      Option.none,
      Option.none,
      errorMessage,
    )

    val headers = fnConvert(recordData)

    headers should contain(
      new RecordHeader(headerNameError, errorMessage.getBytes),
    )

    headers.map(h => h.key()) should not contain oneOf(
      headerNameResponseContent,
      headerNameStatusCode,
    )
  }

}

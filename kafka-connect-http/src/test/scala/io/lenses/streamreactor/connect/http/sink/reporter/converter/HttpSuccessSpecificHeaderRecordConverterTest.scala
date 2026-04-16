/*
 * Copyright 2017-2026 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.http.sink.reporter.converter.HttpSuccessSpecificHeaderRecordConverter._
import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.jdk.StreamConverters._
class HttpSuccessSpecificHeaderRecordConverterTest extends AnyFunSuiteLike with Matchers with OptionValues {

  private val fnConvert: HttpSuccessConnectorSpecificRecordData => Array[Header] =
    in => HttpSuccessSpecificHeaderRecordConverter.apply(originalRecord = in).toScala(Array)

  private val responseContent = "response-content"
  private val statusCode      = 200

  test("should convert HttpSuccessConnectorSpecificRecordData to headers") {

    val recordData = HttpSuccessConnectorSpecificRecordData(
      statusCode,
      Option.of(responseContent),
    )

    val headers = fnConvert(recordData)

    headers should contain allOf (
      new RecordHeader(headerNameResponseContent, responseContent.getBytes),
      new RecordHeader(headerNameStatusCode, String.valueOf(statusCode).getBytes)
    )
  }

  test("should handle empty response content") {

    val recordData = HttpSuccessConnectorSpecificRecordData(
      statusCode,
      Option.none,
    )

    val headers = fnConvert(recordData)

    headers should contain(
      new RecordHeader(headerNameStatusCode, String.valueOf(statusCode).getBytes),
    )

    headers.map(h => h.key()) should not contain headerNameResponseContent
  }
}

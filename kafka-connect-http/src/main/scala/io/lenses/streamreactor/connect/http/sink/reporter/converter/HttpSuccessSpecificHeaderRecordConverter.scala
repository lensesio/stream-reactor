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

import io.lenses.streamreactor.connect.http.sink.reporter.model.HttpSuccessConnectorSpecificRecordData
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.util.function.Function.identity
import java.util.stream.Stream

object HttpSuccessSpecificHeaderRecordConverter {

  val headerNameResponseContent = "response_content"
  val headerNameStatusCode      = "response_status_code"

  /**
    * Converts an HttpSuccessConnectorSpecificRecordData to a Stream of Kafka Headers.
    *
    * @param originalRecord the HttpSuccessConnectorSpecificRecordData to convert
    * @return a Stream of Kafka Headers
    */
  def apply(originalRecord: HttpSuccessConnectorSpecificRecordData): Stream[Header] =
    Stream.of(
      originalRecord.responseContent.map(content =>
        new RecordHeader(headerNameResponseContent, content.getBytes),
      ).stream(),
      Stream.of(
        new RecordHeader(
          headerNameStatusCode,
          String.valueOf(originalRecord.responseStatusCode).getBytes(),
        ),
      ),
    ).flatMap(identity())

}

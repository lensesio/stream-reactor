/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.reporting.model.generic;

import cyclops.control.Try;
import io.lenses.streamreactor.connect.reporting.model.ReportHeadersConstants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class ProducerRecordConverter {

  public static Optional<ProducerRecord<byte[], String>> convert(ReportingRecord source, String reportingTopic) {
    return convertToHeaders(source)
        .flatMap(headers -> createRecord(headers, source, reportingTopic));

  }

  private static Optional<ProducerRecord<byte[], String>> createRecord(List<Header> headers,
      ReportingRecord source, String reportingTopic) {
    return Optional.of(new ProducerRecord<>(reportingTopic, null,
        null, null, source.getPayload(), headers));
  }

  private static Optional<List<Header>> convertToHeaders(ReportingRecord originalRecord) {
    return Try.withCatch(() -> List.<Header>of(
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, originalRecord.getTopicPartition().topic().getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(originalRecord.getTopicPartition()
            .partition()).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(originalRecord.getOffset()).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(originalRecord.getTimestamp())
            .getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, originalRecord.getPayload().getBytes()),
        new RecordHeader(ReportHeadersConstants.ERROR, originalRecord.getError().map(String::getBytes).orElseGet(
            ""::getBytes))
    ), IOException.class)
        .peekFailed(f -> log.warn(
            String.format("Couldn't transform record to Report. Report won't be sent. Topic=%s, Offset=%s",
                originalRecord.getTopicPartition().topic(),
                originalRecord.getOffset()
            ),
            f
        )).toOptional();
  }
}

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
package io.lenses.streamreactor.connect.reporting.model;

import cyclops.control.Option;
import cyclops.control.Try;
import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

@NoArgsConstructor
@Slf4j
public class ProducerRecordConverter {

  private static final Supplier<byte[]> EMPTY_BYTES = ""::getBytes;

  public Option<ProducerRecord<byte[], String>> convert(ReportingRecord source,
      ReportingMessagesConfig messagesConfig) {
    return convertToHeaders(source)
        .flatMap(headers -> createRecord(headers, source, messagesConfig));

  }

  private Option<ProducerRecord<byte[], String>> createRecord(List<Header> headers,
      ReportingRecord source, ReportingMessagesConfig messagesConfig) {
    return Option.of(new ProducerRecord<>(messagesConfig.getReportTopic(),
        messagesConfig.getReportTopicPartition().orElseGet(() -> null), null, null, source.getPayload(), headers));
  }

  private Option<List<Header>> convertToHeaders(ReportingRecord originalRecord) {
    return Try.withCatch(() -> Stream.of(
        buildStandardHeaders(originalRecord),
        getErrorHeader(originalRecord).stream(),
        getResponseContent(originalRecord).stream(),
        getStatusCode(originalRecord).stream()
    )
        .flatMap(identity())
        .collect(Collectors.toUnmodifiableList()), IOException.class)
        .peekFailed(f -> log.warn(
            String.format("Couldn't transform record to Report. Report won't be sent. Topic=%s, Offset=%s",
                originalRecord.getTopicPartition().topic(),
                originalRecord.getOffset()
            ),
            f
        )).toOption();
  }

  private static Stream<Header> buildStandardHeaders(ReportingRecord originalRecord) {
    return Stream.of(
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, originalRecord.getTopicPartition().topic()
            .getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(originalRecord.getTopicPartition()
            .partition()).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(originalRecord.getOffset())
            .getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(originalRecord.getTimestamp())
            .getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, Try.withCatch(() -> originalRecord.getPayload()
            .getBytes()).orElseGet(EMPTY_BYTES))
    );
  }

  private static Option<Header> getErrorHeader(ReportingRecord originalRecord) {
    return originalRecord.getError().map(
        error -> new RecordHeader(ReportHeadersConstants.ERROR, error.getBytes()));
  }

  private static Option<Header> getResponseContent(ReportingRecord originalRecord) {
    return originalRecord.getResponseContent().map(
        error -> new RecordHeader(ReportHeadersConstants.RESPONSE_CONTENT, error.getBytes()));
  }

  private static Option<Header> getStatusCode(ReportingRecord originalRecord) {
    return originalRecord.getResponseStatusCode().map(
        String::valueOf).map(
            error -> new RecordHeader(ReportHeadersConstants.RESPONSE_STATUS, error.getBytes()));
  }
}

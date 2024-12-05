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
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

/**
 * Converts a {@link ReportingRecord} to a {@link ProducerRecord} with headers.
 *
 * @param <C> the type of connector-specific record data
 */
@AllArgsConstructor
@Slf4j
public class RecordConverter<C extends ConnectorSpecificRecordData> {

  private final ReportingMessagesConfig messagesConfig;
  private final Function<C, Stream<Header>> specificDataHeaderConverter;

  private static final Supplier<byte[]> EMPTY_BYTES = ""::getBytes;

  /**
   * Converts a {@link ReportingRecord} to a {@link ProducerRecord}.
   *
   * @param source the reporting record to convert
   * @return an {@link Option} containing the producer record, or {@link Option#none()} if conversion fails
   */
  public Option<ProducerRecord<byte[], String>> convert(ReportingRecord<C> source) {
    val converted =
        convertToHeaders(source)
            .flatMap(headers -> createRecord(headers, source, messagesConfig));
    log.debug("Successfully converted report");
    return converted;
  }

  /**
   * Creates a {@link ProducerRecord} from the given headers and source record.
   *
   * @param headers        the headers to include in the producer record
   * @param source         the source reporting record
   * @param messagesConfig the configuration for reporting messages
   * @return an {@link Option} containing the producer record, or {@link Option#none()} if creation fails
   */
  private Option<ProducerRecord<byte[], String>> createRecord(List<Header> headers,
      ReportingRecord<C> source, ReportingMessagesConfig messagesConfig) {
    return Option.of(new ProducerRecord<>(messagesConfig.getReportTopic(),
        messagesConfig.getReportTopicPartition().orElseGet(() -> null), null, null, source.getPayload(), headers));
  }

  /**
   * Converts the given reporting record to a list of headers.
   *
   * @param originalRecord the reporting record to convert
   * @return an {@link Option} containing the list of headers, or {@link Option#none()} if conversion fails
   */
  private Option<List<Header>> convertToHeaders(ReportingRecord<C> originalRecord) {
    return Try.withCatch(() -> Stream.of(
        buildStandardHeaders(originalRecord),
        specificDataHeaderConverter.apply(originalRecord.getConnectorSpecific())
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

  /**
   * Builds the standard headers for the given reporting record.
   *
   * @param originalRecord the reporting record
   * @return a stream of standard headers
   */
  private Stream<Header> buildStandardHeaders(ReportingRecord<C> originalRecord) {
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

}

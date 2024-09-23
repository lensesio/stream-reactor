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

import static io.lenses.streamreactor.common.util.ByteConverters.toBytes;

import cyclops.control.Option;
import cyclops.control.Try;
import io.lenses.streamreactor.connect.reporting.model.ReportHeadersConstants;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

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
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, toBytes(originalRecord.getTopicPartition().topic())),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, toBytes(originalRecord.getOffset())),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, toBytes(originalRecord.getTimestamp())),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, toBytes(originalRecord.getPayload()))
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

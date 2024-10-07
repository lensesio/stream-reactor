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
import io.lenses.streamreactor.connect.reporting.ReportingMessagesConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.stream.Stream;

import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ProducerRecordConverterTest {

  private static final String REPORTING_TOPIC = "reporting";

  private static final ReportingMessagesConfig messagesConfig =
      new ReportingMessagesConfig(REPORTING_TOPIC, Option.none());
  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 111L;
  private static final long TIMESTAMP = 222L;
  private static final String ENDPOINT = "endpoint.local";
  private static final String JSON_PAYLOAD = "{\"payload\": \"somevalue\"}";
  private static final String ERROR = "Bad things happened";

  private static final ProducerRecordConverter target = new ProducerRecordConverter();

  @ParameterizedTest
  @MethodSource("provideReportingRecords")
  void convertShouldProduceProducerRecord(ReportingRecord reportingRecord, Header[] expectedHeaders) {

    //when
    Option<ProducerRecord<byte[], String>> converted =
        target.convert(reportingRecord, messagesConfig);

    //then
    ProducerRecord<byte[], String> record = getValue(converted);

    assertNotNull(record.headers());
    Header[] headers = record.headers().toArray();
    assertEquals(expectedHeaders.length, headers.length);

    assertThat(record)
        .returns(REPORTING_TOPIC, from(ProducerRecord::topic))
        .returns(null, from(ProducerRecord::partition))
        .returns(null, from(ProducerRecord::timestamp));

    assertArrayEquals(expectedHeaders, headers);
  }

  private static Stream<Object[]> provideReportingRecords() {
    return Stream.of(
        new Object[]{createReportingRecordForAllRequiredFields(), buildExpectedHeadersForRequiredFields()},
        new Object[]{createReportingRecordForAllRequiredAndOptionalFields(), buildExpectedHeadersForAllFields()}
    );
  }

  private static ReportingRecord createReportingRecordForAllRequiredFields() {
    return new ReportingRecord(new TopicPartition(TOPIC, PARTITION), OFFSET,
        TIMESTAMP, ENDPOINT, JSON_PAYLOAD, Collections.emptyList(),
        Option.none(), Option.none(), Option.none()
    );
  }

  private static ReportingRecord createReportingRecordForAllRequiredAndOptionalFields() {
    return new ReportingRecord(new TopicPartition(TOPIC, PARTITION), OFFSET,
        TIMESTAMP, ENDPOINT, JSON_PAYLOAD, Collections.emptyList(),
        Option.of(ERROR), Option.of(200), Option.of("OK")
    );
  }

  private static Header[] buildExpectedHeadersForRequiredFields() {
    return new Header[]{
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, TOPIC.getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(PARTITION).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(OFFSET).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(TIMESTAMP).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, JSON_PAYLOAD.getBytes()),
    };
  }

  private static Header[] buildExpectedHeadersForAllFields() {
    return new Header[]{
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, TOPIC.getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(PARTITION).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(OFFSET).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(TIMESTAMP).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, JSON_PAYLOAD.getBytes()),
        new RecordHeader(ReportHeadersConstants.ERROR, ERROR.getBytes()),
        new RecordHeader(ReportHeadersConstants.RESPONSE_CONTENT, "OK".getBytes()),
        new RecordHeader(ReportHeadersConstants.RESPONSE_STATUS, "200".getBytes()),
    };
  }
}

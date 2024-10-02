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
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerRecordConverterTest {

  private static final String REPORTING_TOPIC = "reporting";
  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 111L;
  private static final long TIMESTAMP = 222L;
  private static final String ENDPOINT = "endpoint.local";
  private static final String JSON_PAYLOAD = "{\"payload\": \"somevalue\"}";
  private static final String ERROR = "Bad things happened";

  private static final ProducerRecordConverter target = new ProducerRecordConverter();

  @Test
  void convertShouldProduceProducerRecord() {
    //given
    ReportingRecord reportingRecord = createReportingRecord();
    ReportingMessagesConfig messagesConfig =
        new ReportingMessagesConfig(REPORTING_TOPIC, Option.none());

    //when
    Option<ProducerRecord<byte[], String>> converted =
        target.convert(reportingRecord, messagesConfig);

    //then
    assertTrue(converted.isPresent());
    ProducerRecord<byte[], String> record = getValue(converted);

    assertNotNull(record.headers());
    Header[] headers = record.headers().toArray();
    assertEquals(7, headers.length);

    assertThat(record)
        .returns(REPORTING_TOPIC, from(ProducerRecord::topic))
        .returns(null, from(ProducerRecord::partition))
        .returns(null, from(ProducerRecord::timestamp));

    assertArrayEquals(buildExpectedHeaders(), headers);
  }

  private Header[] buildExpectedHeaders() {
    return new Header[]{
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, TOPIC.getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(PARTITION).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(OFFSET).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(TIMESTAMP).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, JSON_PAYLOAD.getBytes()),
        new RecordHeader(ReportHeadersConstants.ERROR, "".getBytes())
    };
  }

  private ReportingRecord createReportingRecord() {
    return new ReportingRecord(new TopicPartition(TOPIC, PARTITION), OFFSET,
        TIMESTAMP, ENDPOINT, JSON_PAYLOAD, Collections.emptyList(),
        Option.none()
    );
  }
}

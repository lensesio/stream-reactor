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

import static io.lenses.streamreactor.common.util.ByteConverters.toBytes;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.common.util.ByteConverters;
import java.io.IOException;
import java.util.Optional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class SinkRecordRecordReportTest {

  private static final String REPORT_TOPIC = "reports";
  private static final String RECORD_KEY = "KEY";
  private static final String RECORD_VALUE = "VALUE";
  private static final Long RECORD_TIMESTAMP = 1000L;
  private static final Long RECORD_OFFSET = 2000L;
  private static final String RECORD_TOPIC = "records";

  @Test
  void creationTakesCareOfFields() {
    //given
    SinkRecord sinkRecord = mock(SinkRecord.class);
    String sendingStatus = "OK";

    //when
    SinkRecordRecordReport recordReport = new SinkRecordRecordReport(sinkRecord, sendingStatus);

    //then
    assertEquals(sinkRecord, recordReport.getOriginalRecord());
    assertEquals(sendingStatus, recordReport.getSendingStatus());
  }

  @Test
  void produceReportRecordShouldTranslateOriginalRecordFieldsToReportRecord() {
    //given
    SinkRecord sinkRecord = mock(SinkRecord.class);
    String sendingStatus = "OK";
    when(sinkRecord.key()).thenReturn(RECORD_KEY);
    when(sinkRecord.value()).thenReturn(RECORD_VALUE);
    when(sinkRecord.originalKafkaOffset()).thenReturn(RECORD_OFFSET);
    when(sinkRecord.timestamp()).thenReturn(RECORD_TIMESTAMP);
    when(sinkRecord.originalTopic()).thenReturn(RECORD_TOPIC);

    //when
    Optional<ProducerRecord<byte[], String>> reportRecord =
        new SinkRecordRecordReport(sinkRecord, sendingStatus).produceReportRecord(REPORT_TOPIC);

    //then
    assertTrue(reportRecord.isPresent());
    ProducerRecord<byte[], String> record = reportRecord.get();
    assertEquals(REPORT_TOPIC, record.topic());
    assertEquals(sendingStatus, record.value());
    assertThat(record).matches(rr -> {
      Headers headers = rr.headers();
      try {
        assertThat(headers.toArray())
            .containsExactly(new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, toBytes(RECORD_TOPIC)),
                new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, toBytes(RECORD_OFFSET)),
                new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, toBytes(RECORD_TIMESTAMP)),
                new RecordHeader(ReportHeadersConstants.INPUT_KEY, toBytes(RECORD_KEY)),
                new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, toBytes(RECORD_VALUE))
            );
      } catch (IOException e) {
        return false;
      }
      return true;
    });
  }

  @Test
  void produceReportRecordShouldTranslateToEmptyOptional() {
    //given
    SinkRecord sinkRecord = mock(SinkRecord.class);
    String sendingStatus = "OK";
    when(sinkRecord.originalTopic()).thenReturn(REPORT_TOPIC);

    //when
    Optional<ProducerRecord<byte[], String>> reportRecord;
    try (MockedStatic<ByteConverters> mapper = Mockito.mockStatic(ByteConverters.class)) {
      IOException ioException = new IOException();
      mapper.when(() -> ByteConverters.toBytes(REPORT_TOPIC)).thenThrow(ioException);
      reportRecord = new SinkRecordRecordReport(sinkRecord, sendingStatus).produceReportRecord(REPORT_TOPIC);
    }

    //then
    assertFalse(reportRecord.isPresent());
  }
}

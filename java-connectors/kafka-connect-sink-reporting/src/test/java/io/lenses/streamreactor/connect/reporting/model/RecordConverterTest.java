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
import io.lenses.streamreactor.connect.reporting.TestConnectorSpecificRecordDataData;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.lenses.streamreactor.test.utils.OptionValues.getValue;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordConverterTest {

  private static final String REPORTING_TOPIC = "reporting";

  private static final String TOPIC = "topic";
  private static final int PARTITION = 1;
  private static final long OFFSET = 111L;
  private static final long TIMESTAMP = 222L;
  private static final String ENDPOINT = "endpoint.local";
  private static final String JSON_PAYLOAD = "{\"payload\": \"somevalue\"}";
  private static final String EXTRA_FIELD = "extraField";
  private static final byte[] EXTRA_FIELD_VALUE_BYTES = "extraFieldValue".getBytes();

  @Mock
  private Function<TestConnectorSpecificRecordDataData, Stream<Header>> specificConverter;

  @Mock
  private ReportingMessagesConfig messagesConfig;

  @Mock
  private TestConnectorSpecificRecordDataData specificData;

  @InjectMocks
  private static RecordConverter<TestConnectorSpecificRecordDataData> target;

  @BeforeEach
  public void setUp() {

    when(specificConverter.apply(specificData))
        .thenReturn(
            Stream.of(
                new RecordHeader(EXTRA_FIELD, EXTRA_FIELD_VALUE_BYTES)
            )
        );

    when(messagesConfig.getReportTopic()).thenReturn(REPORTING_TOPIC);
    when(messagesConfig.getReportTopicPartition()).thenReturn(Option.none());
  }

  @Test
  void convertShouldProduceProducerRecord() {

    //when
    Option<ProducerRecord<byte[], String>> converted = target.convert(createReportingRecord());

    //then
    ProducerRecord<byte[], String> producerRecord = getValue(converted);

    assertNotNull(producerRecord.headers());
    Header[] headers = producerRecord.headers().toArray();
    assertThat(producerRecord)
        .returns(REPORTING_TOPIC, from(ProducerRecord::topic))
        .returns(null, from(ProducerRecord::partition))
        .returns(null, from(ProducerRecord::timestamp));

    assertArrayEquals(buildExpectedHeaders(), headers);

    verify(specificConverter).apply(specificData);
  }

  private ReportingRecord<TestConnectorSpecificRecordDataData> createReportingRecord() {

    return new ReportingRecord<>(new TopicPartition(TOPIC, PARTITION), OFFSET,
        TIMESTAMP, ENDPOINT, JSON_PAYLOAD, Collections.emptyList(), specificData
    );
  }

  private Header[] buildExpectedHeaders() {
    return new Header[]{
        new RecordHeader(ReportHeadersConstants.INPUT_TOPIC, TOPIC.getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_PARTITION, String.valueOf(PARTITION).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_OFFSET, String.valueOf(OFFSET).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_TIMESTAMP, String.valueOf(TIMESTAMP).getBytes()),
        new RecordHeader(ReportHeadersConstants.INPUT_KEY, null),
        new RecordHeader(ReportHeadersConstants.INPUT_PAYLOAD, JSON_PAYLOAD.getBytes()),
        new RecordHeader(EXTRA_FIELD, EXTRA_FIELD_VALUE_BYTES)
    };
  }

}

/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.eventhubs.mapping;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.util.Iterator;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class SourceRecordMapperTest {
  private static final String TOPIC = "topic";
  private static final Integer PARTITION = 10;
  private static final Long OFFSET = 111L;
  private static final Long TIMESTAMP = 2024L;
  private static final String HEADER_KEY = "headerKey";
  private static final String OUTPUT_TOPIC = "OUTPUT";

  @Test
  void shouldMapSourceRecordIncludingHeaders() {
    //given
    AzureTopicPartitionKey topicPartitionKey = new AzureTopicPartitionKey(TOPIC, PARTITION);
    AzureOffsetMarker azureOffsetMarker = new AzureOffsetMarker(OFFSET);

    byte[] exampleHeaderValue = new byte[] {1, 10};
    int headerLength = exampleHeaderValue.length;
    Header mockedHeader = mock(Header.class);
    when(mockedHeader.key()).thenReturn(HEADER_KEY);
    when(mockedHeader.value()).thenReturn(exampleHeaderValue);

    Iterator<Header> iterator = singletonList(mockedHeader).iterator();
    Headers mockedHeaders = mock(Headers.class);
    when(mockedHeaders.iterator()).thenReturn(iterator);

    ConsumerRecord<String, String> consumerRecord = mockConsumerRecord(Optional.of(mockedHeaders));

    //when
    Schema stringSchema = Schema.STRING_SCHEMA;
    Schema optionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA;
    SourceRecord sourceRecord = SourceRecordMapper.mapSourceRecordIncludingHeaders(
        consumerRecord, topicPartitionKey, azureOffsetMarker,
        OUTPUT_TOPIC, optionalStringSchema, stringSchema);

    //then
    assertRecordAttributesAreMappedFromSourceConsumerRecord(sourceRecord, consumerRecord,
        OUTPUT_TOPIC, optionalStringSchema, stringSchema, topicPartitionKey, azureOffsetMarker);
    verify(consumerRecord).headers();
    assertThat(sourceRecord.headers()).isNotEmpty().hasSize(1);
    assertThat(((byte[])sourceRecord.headers().lastWithName(HEADER_KEY).value())).hasSize(headerLength);
  }

  @Test
  void mapSourceRecordWithoutHeaders() {
    //given
    AzureTopicPartitionKey topicPartitionKey = new AzureTopicPartitionKey(TOPIC, PARTITION);
    AzureOffsetMarker azureOffsetMarker = new AzureOffsetMarker(OFFSET);

    ConsumerRecord<String, String> consumerRecord = mockConsumerRecord(Optional.empty());

    //when
    Schema stringSchema = Schema.STRING_SCHEMA;
    Schema optionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA;
    SourceRecord sourceRecord = SourceRecordMapper.mapSourceRecordWithoutHeaders(
        consumerRecord, topicPartitionKey, azureOffsetMarker, OUTPUT_TOPIC,
        optionalStringSchema, stringSchema);

    //then
    assertRecordAttributesAreMappedFromSourceConsumerRecord(sourceRecord, consumerRecord,
        OUTPUT_TOPIC, optionalStringSchema, stringSchema, topicPartitionKey, azureOffsetMarker);
    assertThat(sourceRecord.headers()).isEmpty();
  }

  private static ConsumerRecord<String, String> mockConsumerRecord(Optional<Headers> mockedHeaders) {
    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.topic()).thenReturn(TOPIC);
    when(consumerRecord.partition()).thenReturn(PARTITION);
    when(consumerRecord.timestamp()).thenReturn(TIMESTAMP);

    mockedHeaders.ifPresent(headers -> when(consumerRecord.headers()).thenReturn(headers));
    return consumerRecord;
  }

  private void assertRecordAttributesAreMappedFromSourceConsumerRecord(SourceRecord mappedRecord,
      ConsumerRecord<String, String> originalRecord, String outputTopic, Schema keySchema, Schema valueSchema,
      AzureTopicPartitionKey sourcePartitionKey, AzureOffsetMarker offsetMarker) {
    verify(originalRecord).timestamp();
    verify(originalRecord).key();
    verify(originalRecord).value();

    assertThat(mappedRecord)
        .returns(originalRecord.timestamp(), from(SourceRecord::timestamp))
        .returns(sourcePartitionKey, from(SourceRecord::sourcePartition))
        .returns(null, from(SourceRecord::kafkaPartition))
        .returns(offsetMarker, from(SourceRecord::sourceOffset))
        .returns(originalRecord.value(), from(SourceRecord::value))
        .returns(outputTopic, from(SourceRecord::topic))
        .returns(keySchema, from(SourceRecord::keySchema))
        .returns(valueSchema, from(SourceRecord::valueSchema));
  }
}
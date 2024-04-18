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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureOffsetMarker;
import io.lenses.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProvider.AzureTopicPartitionKey;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class SourceRecordMapperTest {
  private final String topic = "topic";
  private final Integer partition = 10;
  private final Long offset = 111L;
  private final Long timestamp = 2024L;

  @Test
  void mapSourceRecordIncludingHeaders() {
    //given
    String headerKey = "headerKey";
    String outputTopic = "OUTPUT";
    byte[] headerValue = new byte[] {1, 10};
    int headerLength = headerValue.length;
    AzureTopicPartitionKey topicPartitionKey = new AzureTopicPartitionKey(topic, partition);
    AzureOffsetMarker azureOffsetMarker = new AzureOffsetMarker(offset);

    Header mockedHeader = mock(Header.class);
    when(mockedHeader.key()).thenReturn(headerKey);
    when(mockedHeader.value()).thenReturn(headerValue);

    Iterator<Header> iterator = singletonList(mockedHeader).iterator();
    Headers mockedHeaders = mock(Headers.class);
    when(mockedHeaders.iterator()).thenReturn(iterator);

    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.headers()).thenReturn(mockedHeaders);
    when(consumerRecord.topic()).thenReturn(topic);
    when(consumerRecord.partition()).thenReturn(partition);
    when(consumerRecord.timestamp()).thenReturn(timestamp);

    //when
    Schema stringSchema = Schema.STRING_SCHEMA;
    Schema optionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA;
    SourceRecord sourceRecord = SourceRecordMapper.mapSourceRecordIncludingHeaders(consumerRecord,
        topicPartitionKey, azureOffsetMarker,
        outputTopic, optionalStringSchema, stringSchema);

    //then
    verify(consumerRecord).timestamp();
    verify(consumerRecord).key();
    verify(consumerRecord).value();
    verify(consumerRecord).headers();
    assertNotNull(sourceRecord);
    assertEquals(outputTopic, sourceRecord.topic());
    assertEquals(timestamp, sourceRecord.timestamp());
    assertNull(sourceRecord.kafkaPartition());
    assertEquals(topicPartitionKey, sourceRecord.sourcePartition());
    assertEquals(azureOffsetMarker, sourceRecord.sourceOffset());
    assertEquals(stringSchema, sourceRecord.valueSchema());
    assertEquals(optionalStringSchema, sourceRecord.keySchema());
    assertNotNull(sourceRecord.headers());
    assertEquals(1, sourceRecord.headers().size());
    assertEquals(headerLength, ((byte[])sourceRecord.headers().lastWithName(headerKey).value()).length);
  }

  @Test
  void mapSourceRecordWithoutHeaders() {
    //given
    AzureTopicPartitionKey topicPartitionKey = new AzureTopicPartitionKey(topic, partition);
    AzureOffsetMarker azureOffsetMarker = new AzureOffsetMarker(offset);
    String outputTopic = "OUTPUT";


    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.topic()).thenReturn(topic);
    when(consumerRecord.partition()).thenReturn(partition);
    when(consumerRecord.timestamp()).thenReturn(timestamp);

    //when
    Schema stringSchema = Schema.STRING_SCHEMA;
    Schema optionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA;
    SourceRecord sourceRecord = SourceRecordMapper.mapSourceRecordWithoutHeaders(consumerRecord,
        topicPartitionKey, azureOffsetMarker, outputTopic,
        optionalStringSchema, stringSchema);

    //then
    verify(consumerRecord).timestamp();
    verify(consumerRecord).key();
    verify(consumerRecord).value();
    assertNotNull(sourceRecord);
    assertEquals(outputTopic, sourceRecord.topic());
    assertEquals(timestamp, sourceRecord.timestamp());
    assertNull(sourceRecord.kafkaPartition());
    assertEquals(0, sourceRecord.headers().size());
    assertEquals(topicPartitionKey, sourceRecord.sourcePartition());
    assertEquals(azureOffsetMarker, sourceRecord.sourceOffset());
    assertEquals(stringSchema, sourceRecord.valueSchema());
    assertEquals(optionalStringSchema, sourceRecord.keySchema());
  }
}
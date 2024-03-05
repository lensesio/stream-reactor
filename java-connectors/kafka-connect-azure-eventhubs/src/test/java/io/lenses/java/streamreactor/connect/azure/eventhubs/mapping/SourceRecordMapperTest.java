package io.lenses.java.streamreactor.connect.azure.eventhubs.mapping;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProviderSingleton.AzureOffsetMarker;
import io.lenses.java.streamreactor.connect.azure.eventhubs.source.TopicPartitionOffsetProviderSingleton.AzureTopicPartitionKey;
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
        optionalStringSchema, stringSchema);

    //then
    verify(consumerRecord).timestamp();
    verify(consumerRecord).topic();
    verify(consumerRecord).key();
    verify(consumerRecord).value();
    verify(consumerRecord).headers();
    assertNotNull(sourceRecord);
    assertEquals(topic, sourceRecord.topic());
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


    ConsumerRecord<String, String> consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.topic()).thenReturn(topic);
    when(consumerRecord.partition()).thenReturn(partition);
    when(consumerRecord.timestamp()).thenReturn(timestamp);

    //when
    Schema stringSchema = Schema.STRING_SCHEMA;
    Schema optionalStringSchema = Schema.OPTIONAL_STRING_SCHEMA;
    SourceRecord sourceRecord = SourceRecordMapper.mapSourceRecordWithoutHeaders(consumerRecord,
        topicPartitionKey, azureOffsetMarker,
        optionalStringSchema, stringSchema);

    //then
    verify(consumerRecord).timestamp();
    verify(consumerRecord).topic();
    verify(consumerRecord).key();
    verify(consumerRecord).value();
    assertNotNull(sourceRecord);
    assertEquals(topic, sourceRecord.topic());
    assertEquals(timestamp, sourceRecord.timestamp());
    assertNull(sourceRecord.kafkaPartition());
    assertEquals(0, sourceRecord.headers().size());
    assertEquals(topicPartitionKey, sourceRecord.sourcePartition());
    assertEquals(azureOffsetMarker, sourceRecord.sourceOffset());
    assertEquals(stringSchema, sourceRecord.valueSchema());
    assertEquals(optionalStringSchema, sourceRecord.keySchema());
  }
}
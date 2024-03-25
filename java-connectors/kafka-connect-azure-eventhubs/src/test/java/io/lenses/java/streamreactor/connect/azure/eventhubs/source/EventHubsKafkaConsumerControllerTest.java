package io.lenses.java.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.java.streamreactor.connect.azure.eventhubs.config.SourceDataType;
import io.lenses.java.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EventHubsKafkaConsumerControllerTest {

  private ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue;

  private EventHubsKafkaConsumerController testObj;


  @BeforeEach
  void setUp() {
    recordsQueue = new ArrayBlockingQueue<>(10);
  }

  @Test
  void pollShouldPollQueueAndReturnSourceRecords() throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    String outputTopic = "OUTPUT";
    List<String> outputTopics = Collections.singletonList(outputTopic);

    SourceDataType mockedDataType = mock(SourceDataType.class);
    when(mockedDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(mockedDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(mockedDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);

    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = Collections.emptyList();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());

    ConsumerRecord consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.headers()).thenReturn(headersMock);
    List<ConsumerRecord<String, String>> consumerRecordList = Collections.singletonList(consumerRecord);

    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    recordsQueue.put(mockedRecords);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        outputTopics);
    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedDataType, times(2)).getSchema();
    verify(mockedBlockingProducer, times(2)).getKeyValueTypes();
    assertNotNull(mockedRecords);
    assertEquals(1, sourceRecords.size());
  }

  @Test
  void pollShouldPollQueueAndReturnSourceRecordsForAllOutputTopics() throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    String outputTopic1 = "OUTPUT1";
    String outputTopic2 = "OUTPUT2";
    List<String> outputTopics = List.of(outputTopic1, outputTopic2);

    SourceDataType mockedDataType = mock(SourceDataType.class);
    when(mockedDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(mockedDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(mockedDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);

    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = Collections.emptyList();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());

    ConsumerRecord consumerRecord = mock(ConsumerRecord.class);
    when(consumerRecord.headers()).thenReturn(headersMock);
    List<ConsumerRecord<String, String>> consumerRecordList = Collections.singletonList(consumerRecord);

    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    recordsQueue.put(mockedRecords);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        outputTopics);
    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedDataType, times(4)).getSchema();
    verify(mockedBlockingProducer, times(4)).getKeyValueTypes();
    assertNotNull(mockedRecords);
    assertEquals(outputTopics.size(), sourceRecords.size());
    for (int i = 0; i < outputTopics.size(); i++) {
      assertEquals(outputTopics.get(i), sourceRecords.get(i).topic());
    }
  }

  @Test
  void closeShouldCloseTheProducer() {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    String outputTopic = "OUTPUT";
    List<String> outputTopics = Collections.singletonList(outputTopic);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        outputTopics);

    //when
    testObj.close(duration);

    //then
    verify(mockedBlockingProducer).stop(duration);
  }
}
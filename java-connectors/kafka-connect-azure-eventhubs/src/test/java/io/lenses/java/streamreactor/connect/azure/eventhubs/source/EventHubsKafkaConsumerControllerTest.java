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
import java.util.ArrayList;
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

  private ArrayBlockingQueue<ConsumerRecords<Object, Object>> recordsQueue;

  private EventHubsKafkaConsumerController testObj;


  @BeforeEach
  void setUp() {
    recordsQueue = new ArrayBlockingQueue<>(10);
  }

  @Test
  void pollShouldPollQueueAndReturnSourceRecords() throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    BlockingQueuedKafkaProducer mockedBlockingProducer = mock(
        BlockingQueuedKafkaProducer.class);
    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    SourceDataType mockedDataType = mock(SourceDataType.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(mockedDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(mockedDataType);
    when(mockedDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue);
    ConsumerRecord consumerRecord = mock(ConsumerRecord.class);
    List<ConsumerRecord<String, String>> consumerRecordList = Collections.singletonList(consumerRecord);
    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = new ArrayList<>();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());
    when(consumerRecord.headers()).thenReturn(headersMock);
    recordsQueue.put(mockedRecords);

    //when

    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedDataType, times(2)).getSchema();
    verify(mockedBlockingProducer, times(2)).getKeyValueTypes();
    assertNotNull(mockedRecords);
    assertEquals(1, sourceRecords.size());
  }

  @Test
  void closeShouldCloseTheProducer() {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    BlockingQueuedKafkaProducer mockedBlockingProducer = mock(
        BlockingQueuedKafkaProducer.class);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue);

    //when
    testObj.close(duration);

    //then
    verify(mockedBlockingProducer).stop(duration);
  }
}
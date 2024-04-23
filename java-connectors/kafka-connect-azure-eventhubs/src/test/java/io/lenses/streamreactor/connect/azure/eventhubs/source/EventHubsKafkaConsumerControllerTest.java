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
package io.lenses.streamreactor.connect.azure.eventhubs.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.eventhubs.config.SourceDataType;
import io.lenses.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

  private static final String INPUT_TOPIC = "INPUT";
  private static final String OUTPUT_TOPIC = "OUTPUT";
  private static final String INPUT_TOPIC_2 = "INPUT2";
  private static final String OUTPUT_TOPIC_2 = "OUTPUT2";
  public static final int DEFAULT_CAPACITY = 10;
  private ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue;

  private EventHubsKafkaConsumerController testObj;


  @BeforeEach
  void setUp() {
    recordsQueue = new ArrayBlockingQueue<>(DEFAULT_CAPACITY);
  }

  @Test
  void pollShouldPollQueueAndReturnSourceRecords() throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    Map<String, String> inputOutputMap = Map.of(INPUT_TOPIC, OUTPUT_TOPIC);

    SourceDataType mockedKeyDataType = mock(SourceDataType.class);
    when(mockedKeyDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);
    SourceDataType mockedValueDataType = mock(SourceDataType.class);
    when(mockedValueDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(mockedKeyDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(mockedValueDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);

    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = Collections.emptyList();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());

    ConsumerRecord consumerRecord = mockConsumerRecord(INPUT_TOPIC, Optional.of(headersMock));
    List<ConsumerRecord<String, String>> consumerRecordList = Collections.singletonList(consumerRecord);

    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    recordsQueue.put(mockedRecords);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);
    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedKeyDataType, times(1)).getSchema();
    verify(mockedValueDataType, times(1)).getSchema();
    verify(mockedBlockingProducer, times(2)).getKeyValueTypes();
    assertNotNull(mockedRecords);
    assertEquals(1, sourceRecords.size());
  }

  @Test
  void pollWithMultipleInputsAndOutputsShouldPollQueueAndReturnSourceRecordsToCorrectOutput()
      throws InterruptedException {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    Map<String, String> inputOutputMap = Map.of(INPUT_TOPIC, OUTPUT_TOPIC, INPUT_TOPIC_2,
        OUTPUT_TOPIC_2);

    SourceDataType mockedDataType = mock(SourceDataType.class);
    when(mockedDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    SourceDataType mockedKeyDataType = mock(SourceDataType.class);
    when(mockedKeyDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);
    SourceDataType mockedValueDataType = mock(SourceDataType.class);
    when(mockedValueDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);

    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(mockedKeyDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(mockedValueDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);

    Headers headersMock = mock(Headers.class);
    List<Header> emptyHeaderList = Collections.emptyList();
    when(headersMock.iterator()).thenReturn(emptyHeaderList.iterator());

    ConsumerRecord consumerRecord = mockConsumerRecord(INPUT_TOPIC, Optional.of(headersMock)
    );
    ConsumerRecord consumerRecord2 = mockConsumerRecord(INPUT_TOPIC_2, Optional.of(headersMock)
    );
    List<ConsumerRecord> consumerRecordList = List.of(consumerRecord, consumerRecord2);

    ConsumerRecords mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());
    recordsQueue.put(mockedRecords);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);
    List<SourceRecord> sourceRecords = testObj.poll(duration);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedKeyDataType, times(2)).getSchema(); //1x both records
    verify(mockedValueDataType, times(2)).getSchema(); //1x both records
    verify(mockedBlockingProducer, times(4)).getKeyValueTypes();
    assertNotNull(mockedRecords);
    assertEquals(2, sourceRecords.size());
    assertEquals(OUTPUT_TOPIC, sourceRecords.get(0).topic());
    assertEquals(OUTPUT_TOPIC_2, sourceRecords.get(1).topic());
  }

  @Test
  void closeShouldCloseTheProducer() {
    //given
    Duration duration = Duration.of(2, ChronoUnit.SECONDS);
    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);

    String inputTopic = "INPUT";
    String outputTopic = "OUTPUT";
    HashMap<String, String> inputOutputMap = new HashMap<>();
    inputOutputMap.put(inputTopic, outputTopic);
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);

    //when
    testObj.close(duration);

    //then
    verify(mockedBlockingProducer).stop(duration);
  }

  private static ConsumerRecord mockConsumerRecord(String inputTopic, Optional<Headers> headersMock) {
    ConsumerRecord consumerRecord = mock(ConsumerRecord.class);
    headersMock.ifPresent(headers -> when(consumerRecord.headers()).thenReturn(headers));
    when(consumerRecord.topic()).thenReturn(inputTopic);
    return consumerRecord;
  }
}
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lenses.streamreactor.connect.azure.eventhubs.config.SourceDataType;
import io.lenses.streamreactor.connect.azure.eventhubs.config.SourceDataType.KeyValueTypes;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class EventHubsKafkaConsumerControllerTest {

  private static final String INPUT_TOPIC = "INPUT";
  private static final String OUTPUT_TOPIC = "OUTPUT";
  private static final String INPUT_TOPIC_2 = "INPUT2";
  private static final String OUTPUT_TOPIC_2 = "OUTPUT2";
  private static final int DEFAULT_CAPACITY = 10;
  private static final Duration DURATION_2_SECONDS = Duration.of(2, ChronoUnit.SECONDS);

  private EventHubsKafkaConsumerController testObj;

  @Test
  void pollShouldPollQueueAndReturnSourceRecords() throws InterruptedException {
    //given
    Map<String, String> inputOutputMap = Map.of(INPUT_TOPIC, OUTPUT_TOPIC);

    SourceDataType mockedKeyDataType = mockSourceDataType();
    SourceDataType mockedValueDataType = mockSourceDataType();

    KeyValueTypes mockedKeyValueTypes = mockKeyValueTypes(mockedKeyDataType, mockedValueDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mockByteBlockingProducer(mockedKeyValueTypes);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue = mockRecordsQueue(INPUT_TOPIC);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);
    List<SourceRecord> sourceRecords = testObj.poll(DURATION_2_SECONDS);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedKeyDataType, times(1)).getSchema();
    verify(mockedValueDataType, times(1)).getSchema();
    verify(mockedBlockingProducer, times(2)).getKeyValueTypes();
    assertEquals(1, sourceRecords.size());
  }

  @Test
  void pollWithMultipleInputsAndOutputsShouldPollQueueAndReturnSourceRecordsToCorrectOutput()
      throws InterruptedException {
    //given
    Map<String, String> inputOutputMap = Map.of(INPUT_TOPIC, OUTPUT_TOPIC, INPUT_TOPIC_2,
        OUTPUT_TOPIC_2);

    SourceDataType mockedKeyDataType = mockSourceDataType();
    SourceDataType mockedValueDataType = mockSourceDataType();

    KeyValueTypes mockedKeyValueTypes = mockKeyValueTypes(mockedKeyDataType, mockedValueDataType);

    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mockByteBlockingProducer(mockedKeyValueTypes);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue = mockRecordsQueue(INPUT_TOPIC, INPUT_TOPIC_2);

    //when
    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);
    List<SourceRecord> sourceRecords = testObj.poll(DURATION_2_SECONDS);

    //then
    verify(mockedBlockingProducer).start();
    verify(mockedKeyDataType, times(2)).getSchema(); //1x both records
    verify(mockedValueDataType, times(2)).getSchema(); //1x both records
    verify(mockedBlockingProducer, times(4)).getKeyValueTypes();
    assertEquals(2, sourceRecords.size());
    assertEquals(OUTPUT_TOPIC, sourceRecords.get(0).topic());
    assertEquals(OUTPUT_TOPIC_2, sourceRecords.get(1).topic());
  }

  @Test
  void closeShouldCloseTheProducer() {
    //given
    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);

    Map<String, String> inputOutputMap = Map.of(INPUT_TOPIC, OUTPUT_TOPIC);

    ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> recordsQueue = mockRecordsQueue();

    testObj = new EventHubsKafkaConsumerController(mockedBlockingProducer, recordsQueue,
        inputOutputMap);

    //when
    testObj.close(DURATION_2_SECONDS);

    //then
    verify(mockedBlockingProducer).stop(DURATION_2_SECONDS);
  }

  private static SourceDataType mockSourceDataType() {
    SourceDataType mockedDataType = mock(SourceDataType.class);
    when(mockedDataType.getSchema()).thenReturn(Schema.OPTIONAL_STRING_SCHEMA);
    return mockedDataType;
  }

  private static KeyValueTypes mockKeyValueTypes(SourceDataType keyDataType, SourceDataType valueDataType) {
    KeyValueTypes mockedKeyValueTypes = mock(KeyValueTypes.class);
    when(mockedKeyValueTypes.getKeyType()).thenReturn(keyDataType);
    when(mockedKeyValueTypes.getValueType()).thenReturn(valueDataType);
    return mockedKeyValueTypes;
  }

  private static ConsumerRecord<byte[], byte[]> mockConsumerRecord(
      String inputTopic, Optional<Headers> headersMock) {
    ConsumerRecord<byte[], byte[]> consumerRecord = mock(ConsumerRecord.class);
    headersMock.ifPresent(headers -> when(consumerRecord.headers()).thenReturn(headers));
    when(consumerRecord.topic()).thenReturn(inputTopic);
    return consumerRecord;
  }

  private static Headers mockEmptyHeaders() {
    Headers headersMock = mock(Headers.class);
    when(headersMock.iterator()).thenReturn(Collections.emptyIterator());
    return headersMock;
  }


  private static KafkaByteBlockingQueuedProducer mockByteBlockingProducer(KeyValueTypes mockedKeyValueTypes) {
    KafkaByteBlockingQueuedProducer mockedBlockingProducer = mock(
        KafkaByteBlockingQueuedProducer.class);
    when(mockedBlockingProducer.getKeyValueTypes()).thenReturn(mockedKeyValueTypes);
    return mockedBlockingProducer;
  }

  private static ArrayBlockingQueue<ConsumerRecords<byte[], byte[]>> mockRecordsQueue
      (String... inputTopics) {

    Headers headersMock = mockEmptyHeaders();

    List<ConsumerRecord<byte[], byte[]>> consumerRecordList = Arrays.stream(inputTopics)
        .map(it -> mockConsumerRecord(it, Optional.of(headersMock)))
        .collect(Collectors.toList());

    ConsumerRecords<byte[], byte[]> mockedRecords = mock(ConsumerRecords.class);
    when(mockedRecords.count()).thenReturn(consumerRecordList.size());
    when(mockedRecords.iterator()).thenReturn(consumerRecordList.iterator());

    return new ArrayBlockingQueue<>(DEFAULT_CAPACITY, false, List.of(mockedRecords));
  }


}
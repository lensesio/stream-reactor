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
package io.lenses.streamreactor.connect.azure.servicebus.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskToReceiverBridgeTest {

  Map<String, String> properties = new HashMap<>();
  ArrayBlockingQueue<SourceRecord> blockingQueue;
  OffsetStorageReader offsetReader;

  TaskToReceiverBridge testObj;

  @BeforeEach
  void setUp() {
    blockingQueue = mock(ArrayBlockingQueue.class);
    offsetReader = mock(OffsetStorageReader.class);
  }

  @Test
  void closeReceiversShouldCloseAllReceivers() {
    //given
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    ServiceBusReceiverFacade receiver2 = mock(ServiceBusReceiverFacade.class);
    List<ServiceBusReceiverFacade> receivers = List.of(receiver1, receiver2);

    //when
    testObj = new TaskToReceiverBridge(blockingQueue, offsetReader, receivers);
    testObj.closeReceivers();

    //then
    verify(receiver1).unsubscribeAndClose();
    verify(receiver2).unsubscribeAndClose();
  }

  @Test
  void pollShouldDrainAllMessagesFromQueue() throws InterruptedException {
    //given
    int arrayBlockingQueueCapacity = 10;
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    List<SourceRecord> allSourceRecords = new ArrayList<>(arrayBlockingQueueCapacity);
    BlockingQueue<SourceRecord> sourceRecordBlockingQueue = new ArrayBlockingQueue<>(arrayBlockingQueueCapacity);
    List<ServiceBusReceiverFacade> receivers = List.of(receiver1);

    for (int i = 0; i < arrayBlockingQueueCapacity; i++) {
      SourceRecord mockedRecord = mock(SourceRecord.class);
      allSourceRecords.add(mockedRecord);
      sourceRecordBlockingQueue.put(mockedRecord);
    }

    //when
    testObj = new TaskToReceiverBridge(sourceRecordBlockingQueue, offsetReader, receivers);
    List<SourceRecord> polled = testObj.poll();

    //then
    assertThat(polled).hasSize(arrayBlockingQueueCapacity);
    assertThat(polled).containsExactlyElementsOf(allSourceRecords);
  }
}

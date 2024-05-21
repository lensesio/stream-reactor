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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TaskToReceiverBridgeTest {

  Map<String, String> properties = new HashMap<>();
  ArrayBlockingQueue<ServiceBusMessageHolder> blockingQueue;
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
    ExecutorService executorService = mock(ExecutorService.class);
    String receiverId1 = "RECEIVER1";
    String receiverId2 = "RECEIVER2";
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    ServiceBusReceiverFacade receiver2 = mock(ServiceBusReceiverFacade.class);
    when(receiver1.getReceiverId()).thenReturn(receiverId1);
    when(receiver2.getReceiverId()).thenReturn(receiverId2);
    Map<String, ServiceBusReceiverFacade> receivers =
        Map.of(
            receiverId1, receiver1,
            receiverId2, receiver2
        );

    //when
    testObj = new TaskToReceiverBridge(blockingQueue, offsetReader, receivers, executorService);
    testObj.closeReceivers();

    //then
    verify(receiver1).unsubscribeAndClose();
    verify(receiver2).unsubscribeAndClose();
  }

  @Test
  void pollShouldDrainAllMessagesFromQueue() throws InterruptedException {
    //given
    int arrayBlockingQueueCapacity = 10;
    String receiverId1 = "RECEIVER1";
    String messageIdTemplate = "MSGID%s";
    ExecutorService executorService = mock(ExecutorService.class);
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    List<SourceRecord> allSourceRecords = new ArrayList<>(arrayBlockingQueueCapacity);
    BlockingQueue<ServiceBusMessageHolder> sourceRecordBlockingQueue =
        new ArrayBlockingQueue<>(arrayBlockingQueueCapacity);
    Map<String, ServiceBusReceiverFacade> receivers = Map.of(receiverId1, receiver1);

    for (int i = 0; i < arrayBlockingQueueCapacity; i++) {
      ServiceBusMessageHolder mockedRecord = mock(ServiceBusMessageHolder.class);
      ServiceBusReceivedMessage busReceivedMessage = mock(ServiceBusReceivedMessage.class);
      SourceRecord sourceRecord = mock(SourceRecord.class);
      when(mockedRecord.getTranslatedRecord()).thenReturn(sourceRecord);
      when(mockedRecord.getOriginalRecord()).thenReturn(busReceivedMessage);
      when(busReceivedMessage.getMessageId()).thenReturn(String.format(messageIdTemplate, i));
      allSourceRecords.add(sourceRecord);
      sourceRecordBlockingQueue.put(mockedRecord);
    }

    //when
    testObj = new TaskToReceiverBridge(sourceRecordBlockingQueue, offsetReader, receivers, executorService);
    List<SourceRecord> polled = testObj.poll();

    //then
    assertThat(polled).hasSize(arrayBlockingQueueCapacity);
    assertThat(polled).containsExactlyElementsOf(allSourceRecords);
  }

  @Test
  void commitMessageInServiceBusShouldCallExecutorService() {
    //given
    int arrayBlockingQueueCapacity = 10;
    String receiverId1 = "RECEIVER1";
    String messageKey = "MESSAGE_KEY";
    ExecutorService executorService = mock(ExecutorService.class);
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    BlockingQueue<ServiceBusMessageHolder> sourceRecordBlockingQueue =
        new ArrayBlockingQueue<>(arrayBlockingQueueCapacity);
    Map<String, ServiceBusReceiverFacade> receivers = Map.of(receiverId1, receiver1);
    SourceRecord sourceRecord = mock(SourceRecord.class);
    when(sourceRecord.key()).thenReturn(messageKey);

    //when
    testObj = new TaskToReceiverBridge(sourceRecordBlockingQueue, offsetReader, receivers, executorService);
    testObj.commitRecordInServiceBus(sourceRecord);

    //then
    verify(executorService).submit(any(Runnable.class));
  }
}

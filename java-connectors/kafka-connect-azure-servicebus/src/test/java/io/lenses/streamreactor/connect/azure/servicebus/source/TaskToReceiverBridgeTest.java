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
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

@ExtendWith(MockitoExtension.class)
class TaskToReceiverBridgeTest {

  private static final String RECEIVER_ID_1 = "RECEIVER1";
  private static final String RECEIVER_ID_2 = "RECEIVER2";
  @Mock
  private ArrayBlockingQueue<ServiceBusMessageHolder> blockingQueue;
  private TaskToReceiverBridge testObj;

  @Test
  void closeReceiversShouldCloseAllReceivers() {

    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);
    ServiceBusReceiverFacade receiver2 = mock(ServiceBusReceiverFacade.class);
    Map<String, ServiceBusReceiverFacade> receivers =
        Map.of(
            RECEIVER_ID_1, receiver1,
            RECEIVER_ID_2, receiver2
        );

    //when
    testObj = new TaskToReceiverBridge(blockingQueue, receivers);
    testObj.closeReceivers();

    //then
    verify(receiver1).unsubscribeAndClose();
    verify(receiver2).unsubscribeAndClose();
  }

  @Test
  void pollShouldDrainAllMessagesFromQueue() {
    //given
    int arrayBlockingQueueCapacity = 10;
    String messageIdTemplate = "MSGID%s";
    BlockingQueue<ServiceBusMessageHolder> sourceRecordBlockingQueue =
        new ArrayBlockingQueue<>(arrayBlockingQueueCapacity);

    ExecutorService executorService = mock(ExecutorService.class);
    ServiceBusReceiverFacade receiver1 = mock(ServiceBusReceiverFacade.class);

    Map<String, ServiceBusReceiverFacade> receivers = Map.of(RECEIVER_ID_1, receiver1);

    List<SourceRecord> allSourceRecords = IntStream.range(0, arrayBlockingQueueCapacity).mapToObj(i -> {
      String formattedMessageId = String.format(messageIdTemplate, i);
      return createMockedSourceRecord(formattedMessageId, sourceRecordBlockingQueue);
    }).collect(Collectors.toList());

    //when
    testObj = new TaskToReceiverBridge(sourceRecordBlockingQueue, receivers);
    List<SourceRecord> polled = testObj.poll();

    //then
    assertThat(polled).hasSize(arrayBlockingQueueCapacity).containsExactlyElementsOf(allSourceRecords);
  }

  private static SourceRecord createMockedSourceRecord(String format,
      BlockingQueue<ServiceBusMessageHolder> sourceRecordBlockingQueue) {
    ServiceBusReceivedMessage busReceivedMessage = mock(ServiceBusReceivedMessage.class);
    when(busReceivedMessage.getMessageId()).thenReturn(format);

    SourceRecord sourceRecord = mock(SourceRecord.class);

    ServiceBusMessageHolder mockedRecord = mock(ServiceBusMessageHolder.class);
    when(mockedRecord.getOriginalRecord()).thenReturn(busReceivedMessage);
    when(mockedRecord.getTranslatedRecord()).thenReturn(sourceRecord);

    try {
      sourceRecordBlockingQueue.put(mockedRecord);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return sourceRecord;
  }
}

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

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.AzureServiceBusSourceRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static io.lenses.streamreactor.connect.azure.servicebus.source.Helper.createMockedServiceBusMessage;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AzureServiceBusSourceTaskTest {

  @Test
  void pollShouldPollTheBridgeAndReturnNullIfEmpty() {
    TaskToReceiverBridge taskToReceiverBridge = mock(TaskToReceiverBridge.class);
    AzureServiceBusSourceTask task = new AzureServiceBusSourceTask();
    task.initialize(taskToReceiverBridge);
    //given
    when(taskToReceiverBridge.poll()).thenReturn(emptyList());
    List<SourceRecord> poll = task.poll();

    //then
    assertNull(poll);
  }

  @Test
  void pollShouldPollTheBridgeAndReturnListOfRecordsIfNotEmpty() {
    TaskToReceiverBridge taskToReceiverBridge = mock(TaskToReceiverBridge.class);
    AzureServiceBusSourceTask task = new AzureServiceBusSourceTask();
    task.initialize(taskToReceiverBridge);
    //given
    SourceRecord mockedRecord = mock(SourceRecord.class);
    List<SourceRecord> sourceRecords = singletonList(mockedRecord);
    when(taskToReceiverBridge.poll()).thenReturn(sourceRecords);

    //when
    List<SourceRecord> poll = task.poll();

    //then
    verify(taskToReceiverBridge).poll();
    assertNotNull(poll);
    assertEquals(sourceRecords, poll);
  }

  @Test
  void stopShouldCloseBridgeReceivers() {
    TaskToReceiverBridge taskToReceiverBridge = mock(TaskToReceiverBridge.class);
    AzureServiceBusSourceTask task = new AzureServiceBusSourceTask();
    task.initialize(taskToReceiverBridge);
    //given

    //when
    task.stop();

    //then
    verify(taskToReceiverBridge).closeReceivers();
  }

  @Test
  void returnTheSequenceOfRecordsEnqueued() {
    final ServiceBusReceivedMessage message1 =
        createMockedServiceBusMessage(1, OffsetDateTime.now(), Duration.ofSeconds(10));
    final ServiceBusReceivedMessage message2 =
        createMockedServiceBusMessage(2, OffsetDateTime.now(), Duration.ofSeconds(10));
    final String receivedId = "RECEIVER1";
    final BlockingQueue<ServiceBusMessageHolder> recordsQueue = new ArrayBlockingQueue<>(10);
    final String inputBus = "inputBus";
    final String outputTopic = "outputTopic";
    ServiceBusReceiverFacade.onSuccessfulMessage(receivedId, recordsQueue, inputBus, outputTopic).accept(message1);
    ServiceBusReceiverFacade.onSuccessfulMessage(receivedId, recordsQueue, inputBus, outputTopic).accept(message2);
    final TaskToReceiverBridge taskToReceiverBridge = new TaskToReceiverBridge(recordsQueue, Collections.emptyMap());
    final AzureServiceBusSourceTask task = new AzureServiceBusSourceTask();
    task.initialize(taskToReceiverBridge);

    final List<SourceRecord> actualRecords = task.poll();
    assertEquals(2, actualRecords.size());

    final AzureServiceBusSourceRecord sourceRecord1 = (AzureServiceBusSourceRecord) actualRecords.get(0);
    assertEquals("messageId1", sourceRecord1.key());

    final AzureServiceBusSourceRecord sourceRecord2 = (AzureServiceBusSourceRecord) actualRecords.get(1);
    assertEquals("messageId2", sourceRecord2.key());

  }
}

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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureServiceBusSourceTaskTest {

  private TaskToReceiverBridge taskToReceiverBridge;
  private AzureServiceBusSourceTask testObj;

  @BeforeEach
  void setUp() {
    taskToReceiverBridge = mock(TaskToReceiverBridge.class);
    testObj = new AzureServiceBusSourceTask();
    testObj.initialize(taskToReceiverBridge);
  }

  @Test
  void pollShouldPollTheBridgeAndReturnNullIfEmpty() {
    //given
    when(taskToReceiverBridge.poll()).thenReturn(emptyList());

    //when
    List<SourceRecord> poll = testObj.poll();

    //then
    assertNull(poll);
  }

  @Test
  void pollShouldPollTheBridgeAndReturnListOfRecordsIfNotEmpty() {
    //given
    SourceRecord mockedRecord = mock(SourceRecord.class);
    List<SourceRecord> sourceRecords = singletonList(mockedRecord);
    when(taskToReceiverBridge.poll()).thenReturn(sourceRecords);

    //when
    List<SourceRecord> poll = testObj.poll();

    //then
    verify(taskToReceiverBridge).poll();
    assertNotNull(poll);
    assertEquals(sourceRecords, poll);
  }

  @Test
  void stopShouldCloseBridgeReceivers() {
    //given

    //when
    testObj.stop();

    //then
    verify(taskToReceiverBridge).closeReceivers();
  }
}

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
package io.lenses.streamreactor.connect.azure.servicebus.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusMessageBatch;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ServiceBusSenderFacadeTest {

  private static final String ORIGINAL_TOPIC_NAME = "KAFKA_TOPIC";
  private static final ServiceBusSenderClient SERVICE_BUS_SENDER_CLIENT = mock(ServiceBusSenderClient.class);
  private static Consumer<Map<TopicPartition, OffsetAndMetadata>> consumerFunction;
  private ServiceBusSenderFacade testObj;

  @BeforeEach
  void setUp() {
    consumerFunction = mock(Consumer.class);
    testObj = new ServiceBusSenderFacade(consumerFunction, ORIGINAL_TOPIC_NAME, SERVICE_BUS_SENDER_CLIENT, true);
  }

  @Test
  void initializePartitionShouldInitializeInternalOffsetsForPartition() {
    //given
    Integer partition = 101;
    TopicPartition topicPartition = mock(TopicPartition.class);
    when(topicPartition.partition()).thenReturn(partition);

    //when
    testObj.initializePartition(topicPartition);

    //then
    verify(topicPartition).partition();
  }

  @Test
  void closeShouldCloseSender() {
    //when`
    testObj.close();

    //then
    verify(SERVICE_BUS_SENDER_CLIENT).close();
  }

  @Test
  void sendMessagesShouldSendNicelyAndUpdateOffsets() {
    //given
    final long offset = 101L;
    final int partition = 5;
    final TopicPartition topicPartition = new TopicPartition(ORIGINAL_TOPIC_NAME, partition);
    final ServiceBusMessage busMessage = mock(ServiceBusMessage.class);

    ServiceBusMessageWrapper composite = mock(ServiceBusMessageWrapper.class);
    when(composite.getOriginalKafkaOffset()).thenReturn(offset);
    when(composite.getOriginalKafkaPartition()).thenReturn(partition);
    when(composite.getServiceBusMessage()).thenReturn(Optional.of(busMessage));

    ServiceBusMessageBatch senderMessageBatch = mock(ServiceBusMessageBatch.class);
    when(senderMessageBatch.getCount()).thenReturn(1);
    when(SERVICE_BUS_SENDER_CLIENT.createMessageBatch()).thenReturn(senderMessageBatch);

    //when
    Optional<ServiceBusException> serviceBusException = testObj.sendMessages(List.of(composite));

    //then
    assertTrue(serviceBusException.isEmpty());
    verify(senderMessageBatch).tryAddMessage(busMessage);
    verify(SERVICE_BUS_SENDER_CLIENT).sendMessages(senderMessageBatch);
    verify(consumerFunction).accept(argThat(offsetMap -> offsetMap.get(topicPartition).offset() == offset));
  }

  @Test
  void sendMessagesShouldSendSeparatelyAndUpdateOffsetsIfBatchingDisabled() {
    //given
    Mockito.reset(SERVICE_BUS_SENDER_CLIENT);
    testObj = new ServiceBusSenderFacade(consumerFunction, ORIGINAL_TOPIC_NAME, SERVICE_BUS_SENDER_CLIENT, false);

    final long offset = 101L;
    final int partition = 5;
    final TopicPartition topicPartition = new TopicPartition(ORIGINAL_TOPIC_NAME, partition);
    final ServiceBusMessage busMessage = mock(ServiceBusMessage.class);

    ServiceBusMessageWrapper composite = mock(ServiceBusMessageWrapper.class);
    when(composite.getOriginalKafkaOffset()).thenReturn(offset);
    when(composite.getOriginalKafkaPartition()).thenReturn(partition);
    when(composite.getServiceBusMessage()).thenReturn(Optional.of(busMessage));

    //when
    Optional<ServiceBusException> serviceBusException = testObj.sendMessages(List.of(composite));

    //then
    assertTrue(serviceBusException.isEmpty());
    verify(SERVICE_BUS_SENDER_CLIENT, never()).createMessageBatch();
    verify(SERVICE_BUS_SENDER_CLIENT).sendMessages(anyList());
    verify(consumerFunction).accept(argThat(offsetMap -> offsetMap.get(topicPartition).offset() == offset));
  }

  @Test
  void sendMessagesShouldUpdateOffsetsWithoutSendingMessageIfPayloadNull() {
    //given
    final long offset = 101L;
    final int partition = 5;
    final TopicPartition topicPartition = new TopicPartition(ORIGINAL_TOPIC_NAME, partition);
    final ServiceBusMessage busMessage = mock(ServiceBusMessage.class);

    ServiceBusMessageWrapper composite = mock(ServiceBusMessageWrapper.class);
    when(composite.getOriginalKafkaOffset()).thenReturn(offset);
    when(composite.getOriginalKafkaPartition()).thenReturn(partition);
    when(composite.getServiceBusMessage()).thenReturn(Optional.empty());

    ServiceBusMessageBatch senderMessageBatch = mock(ServiceBusMessageBatch.class);
    when(senderMessageBatch.getCount()).thenReturn(0);
    when(SERVICE_BUS_SENDER_CLIENT.createMessageBatch()).thenReturn(senderMessageBatch);

    //when
    Optional<ServiceBusException> serviceBusException = testObj.sendMessages(List.of(composite));

    //then
    assertTrue(serviceBusException.isEmpty());
    verify(senderMessageBatch, never()).tryAddMessage(busMessage);
    verify(SERVICE_BUS_SENDER_CLIENT, never()).sendMessages(senderMessageBatch);
    verify(consumerFunction).accept(argThat(offsetMap -> offsetMap.get(topicPartition).offset() == offset));
  }

  @Test
  void sendMessagesShouldReturnExceptionIfSendingFailed() {
    //given
    final long offset = 101L;
    final int partition = 5;
    final ServiceBusMessage busMessage = mock(ServiceBusMessage.class);
    ServiceBusException busException = mock(ServiceBusException.class);

    ServiceBusMessageWrapper composite = mock(ServiceBusMessageWrapper.class);
    when(composite.getOriginalKafkaOffset()).thenReturn(offset);
    when(composite.getOriginalKafkaPartition()).thenReturn(partition);
    when(composite.getServiceBusMessage()).thenReturn(Optional.of(busMessage));

    ServiceBusMessageBatch senderMessageBatch = mock(ServiceBusMessageBatch.class);
    when(SERVICE_BUS_SENDER_CLIENT.createMessageBatch()).thenReturn(senderMessageBatch);
    when(senderMessageBatch.getCount()).thenReturn(1);
    doThrow(busException).when(SERVICE_BUS_SENDER_CLIENT).sendMessages(senderMessageBatch);

    //when
    Optional<ServiceBusException> serviceBusException = testObj.sendMessages(List.of(composite));

    //then
    assertFalse(serviceBusException.isEmpty());
    assertEquals(serviceBusException.get(), busException);
  }
}

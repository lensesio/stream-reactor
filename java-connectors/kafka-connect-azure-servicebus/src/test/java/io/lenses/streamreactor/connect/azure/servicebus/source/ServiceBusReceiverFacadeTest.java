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

import com.azure.core.amqp.models.AmqpAnnotatedMessage;
import com.azure.core.amqp.models.AmqpMessageBody;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.AzureServiceBusSourceRecord;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusToSourceRecordMapper;
import io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusKcqlProperties;
import io.lenses.streamreactor.connect.azure.servicebus.util.ServiceBusType;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.RetrySpec;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import static io.lenses.streamreactor.connect.azure.servicebus.source.Helper.createMockedServiceBusMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ServiceBusReceiverFacadeTest {

  private static final String SOME_RECEIVER_ID = "REC-IEVER-ID";
  private final BlockingQueue<ServiceBusMessageHolder> mockedQueue = mock(BlockingQueue.class);

  @Test
  void shouldThrowIllegalArgumentExceptionOnCreationWithBadlyFormattedConnectionString() {
    //given
    String badFormatConnectionString = "connectionString";
    Map<String, String> properties = new HashMap<>();
    properties.put(ServiceBusKcqlProperties.SERVICE_BUS_TYPE.getPropertyName(), ServiceBusType.TOPIC.name());

    Kcql kcql = mock(Kcql.class);
    when(kcql.getProperties()).thenReturn(properties);

    //when
    assertThrows(IllegalArgumentException.class, () -> ServiceBusReceiverFacade.buildAsyncClient(kcql,
        badFormatConnectionString, 1000)
    );
  }

  @Test
  void completeShouldCallReceiverComplete() {
    //given
    ServiceBusReceiverAsyncClient receiverAsyncClient = mock(ServiceBusReceiverAsyncClient.class);
    when(receiverAsyncClient.receiveMessages()).thenReturn(Flux.empty());
    ServiceBusReceivedMessage mockedMessage = mock(ServiceBusReceivedMessage.class);
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    AtomicReference<Integer> ackCounter = new AtomicReference<>(0);
    MessageAck msgAck = (mono, msgId) -> ackCounter.getAndSet(ackCounter.get() + 1);
    //when
    ServiceBusReceiverFacade serviceBusReceiverFacade =
        new ServiceBusReceiverFacade(SOME_RECEIVER_ID,
            receiverAsyncClient,
            ServiceBusReceiverFacade
                .onSuccessfulMessage(SOME_RECEIVER_ID, mockedQueue, "from", "to"),
            ServiceBusReceiverFacade.onError(SOME_RECEIVER_ID, serviceBusReceiverError),
            msgAck);
    serviceBusReceiverFacade.complete(mockedMessage);

    //then
    verify(receiverAsyncClient).complete(mockedMessage);
    assert ackCounter.get() == 1;
  }

  @Test
  void failingCompleteShouldCallOnError() {
    //given
    ServiceBusReceiverAsyncClient receiverAsyncClient = mock(ServiceBusReceiverAsyncClient.class);
    final Throwable exception = new RuntimeException("Error");
    when(receiverAsyncClient.receiveMessages()).thenReturn(Flux.error(exception));
    AtomicReference<Throwable> serviceBusReceiverError = new AtomicReference<>();
    MessageAck msgAck = RetryMessageAck.create(RetrySpec.backoff(3, Duration.ofMillis(100)));

    ServiceBusReceiverFacade serviceBusReceiverFacade =
        new ServiceBusReceiverFacade(SOME_RECEIVER_ID,
            receiverAsyncClient,
            ServiceBusReceiverFacade
                .onSuccessfulMessage(SOME_RECEIVER_ID, mockedQueue, "from", "to"),
            ServiceBusReceiverFacade.onError(SOME_RECEIVER_ID, serviceBusReceiverError),
            msgAck);

    //then
    final Throwable actualError = serviceBusReceiverError.get();
    assertEquals(exception, actualError);
  }

  @Test
  void onSuccessfulMessage() {
    //given
    String receivedId = "receivedId";
    BlockingQueue<ServiceBusMessageHolder> recordsQueue = new ArrayBlockingQueue<>(10);
    String inputBus = "inputBus";
    String outputTopic = "outputTopic";
    ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
    when(message.getMessageId()).thenReturn("messageId");
    when(message.getContentType()).thenReturn("contentType");
    when(message.getCorrelationId()).thenReturn("correlationId");
    when(message.getReplyTo()).thenReturn("replyTo");
    when(message.getReplyToSessionId()).thenReturn("replyToSessionId");
    when(message.getDeadLetterSource()).thenReturn("deadLetterSource");
    when(message.getSessionId()).thenReturn("sessionId");
    when(message.getLockToken()).thenReturn("lockToken");
    when(message.getSequenceNumber()).thenReturn(1L);
    when(message.getPartitionKey()).thenReturn("partitionKey");
    final OffsetDateTime enqueuedTime = OffsetDateTime.now();
    when(message.getEnqueuedTime()).thenReturn(enqueuedTime);
    when(message.getDeliveryCount()).thenReturn(1L);
    final Duration ttl = Duration.ofSeconds(10);
    when(message.getTimeToLive()).thenReturn(ttl);
    when(message.getTo()).thenReturn("to");
    final AmqpAnnotatedMessage raw = mock(AmqpAnnotatedMessage.class);
    when(message.getRawAmqpMessage()).thenReturn(raw);
    final AmqpMessageBody body = AmqpMessageBody.fromData(new byte[]{1, 2, 3});
    when(raw.getBody()).thenReturn(body);

    //when
    ServiceBusReceiverFacade.onSuccessfulMessage(receivedId, recordsQueue, inputBus, outputTopic).accept(message);

    //then
    List<ServiceBusMessageHolder> results = new ArrayList<>(10);
    int drained = recordsQueue.drainTo(results);
    assertEquals(1, drained);
    ServiceBusMessageHolder serviceBusMessageHolder = results.get(0);
    assertEquals(receivedId, serviceBusMessageHolder.getReceiverId());
    assertEquals(message, serviceBusMessageHolder.getOriginalRecord());

    final AzureServiceBusSourceRecord record =
        (AzureServiceBusSourceRecord) serviceBusMessageHolder.getTranslatedRecord();
    assertEquals(outputTopic, record.topic());
    assertEquals("messageId", record.key());

    final Struct value = (Struct) record.value();
    assertEquals(ServiceBusToSourceRecordMapper.VALUE_SCHEMA, value.schema());
    assertEquals(1L, value.getInt64(ServiceBusValueSchemaField.SchemaFieldConstants.DELIVERY_COUNT));
    assertEquals(enqueuedTime.toInstant().toEpochMilli(), value.getInt64(
        ServiceBusValueSchemaField.SchemaFieldConstants.ENQUEUED_TIME_UTC));
    assertEquals("contentType", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.CONTENT_TYPE));
    assertEquals("AzureServiceBusSourceConnector", value.getString(
        ServiceBusValueSchemaField.SchemaFieldConstants.LABEL));
    assertEquals("correlationId", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.CORRELATION_ID));
    assertEquals("replyTo", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.REPLY_TO));
    assertEquals("replyToSessionId", value.getString(
        ServiceBusValueSchemaField.SchemaFieldConstants.REPLY_TO_SESSION_ID));
    assertEquals("deadLetterSource", value.getString(
        ServiceBusValueSchemaField.SchemaFieldConstants.DEAD_LETTER_SOURCE));
    assertEquals(1L, value.getInt64(ServiceBusValueSchemaField.SchemaFieldConstants.SEQUENCE_NUMBER));
    assertEquals("sessionId", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.SESSION_ID));
    assertEquals("lockToken", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.LOCK_TOKEN));
    assertEquals("to", value.getString(ServiceBusValueSchemaField.SchemaFieldConstants.GET_TO));
    assertEquals(10_000, value.getInt64(ServiceBusValueSchemaField.SchemaFieldConstants.TIME_TO_LIVE));
    byte[] expected = new byte[]{1, 2, 3};
    byte[] actual = value.getBytes(ServiceBusValueSchemaField.SchemaFieldConstants.MESSAGE_BODY);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }

  }

  @Test
  void onSuccessfulMessageWithTwoCallsRetainsTheMessageOrder() {
    //given
    String receivedId = "receivedId";
    BlockingQueue<ServiceBusMessageHolder> recordsQueue = new ArrayBlockingQueue<>(10);
    String inputBus = "inputBus";
    String outputTopic = "outputTopic";
    ServiceBusReceivedMessage message1 = createMockedServiceBusMessage(1, OffsetDateTime.now(), Duration.ofSeconds(10));
    ServiceBusReceivedMessage message2 = createMockedServiceBusMessage(2, OffsetDateTime.now(), Duration.ofSeconds(10));

    //when
    ServiceBusReceiverFacade.onSuccessfulMessage(receivedId, recordsQueue, inputBus, outputTopic).accept(message1);
    ServiceBusReceiverFacade.onSuccessfulMessage(receivedId, recordsQueue, inputBus, outputTopic).accept(message2);

    //then
    List<ServiceBusMessageHolder> results = new ArrayList<>(10);
    int drained = recordsQueue.drainTo(results);
    assertEquals(2, drained);
    assertEquals("messageId1", results.get(0).getOriginalRecord().getMessageId());
    assertEquals("messageId2", results.get(1).getOriginalRecord().getMessageId());
  }

}

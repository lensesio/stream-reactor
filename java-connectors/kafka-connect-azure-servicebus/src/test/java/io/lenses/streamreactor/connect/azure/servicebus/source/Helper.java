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

import java.time.Duration;
import java.time.OffsetDateTime;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Helper {

  public static ServiceBusReceivedMessage createMockedServiceBusMessage(int number, OffsetDateTime enqueuedTime,
      Duration ttl) {
    ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
    when(message.getMessageId()).thenReturn("messageId" + number);
    when(message.getContentType()).thenReturn("contentType" + number);
    when(message.getCorrelationId()).thenReturn("correlationId" + number);
    when(message.getReplyTo()).thenReturn("replyTo" + number);
    when(message.getReplyToSessionId()).thenReturn("replyToSessionId" + number);
    when(message.getDeadLetterSource()).thenReturn("deadLetterSource" + number);
    when(message.getSessionId()).thenReturn("sessionId" + number);
    when(message.getLockToken()).thenReturn("lockToken" + number);
    when(message.getSequenceNumber()).thenReturn((long) number);
    when(message.getPartitionKey()).thenReturn("partitionKey" + number);
    when(message.getEnqueuedTime()).thenReturn(enqueuedTime);
    when(message.getDeliveryCount()).thenReturn((long) number);
    when(message.getTimeToLive()).thenReturn(ttl);
    when(message.getTo()).thenReturn("to" + number);
    final AmqpAnnotatedMessage raw = mock(AmqpAnnotatedMessage.class);
    when(message.getRawAmqpMessage()).thenReturn(raw);
    final AmqpMessageBody body =
        AmqpMessageBody.fromData(new byte[]{(byte) number, (byte) (number >> 8), (byte) (number >> 16)});
    when(raw.getBody()).thenReturn(body);
    return message;
  }
}

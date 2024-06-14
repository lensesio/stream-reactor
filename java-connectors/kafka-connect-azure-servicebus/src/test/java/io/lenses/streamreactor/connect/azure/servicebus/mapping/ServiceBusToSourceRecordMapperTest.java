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
package io.lenses.streamreactor.connect.azure.servicebus.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

import io.lenses.streamreactor.connect.azure.servicebus.source.AzureServiceBusSourceConnector;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusOffsetMarker;
import io.lenses.streamreactor.connect.azure.servicebus.source.ServiceBusPartitionOffsetProvider.AzureServiceBusPartitionKey;

class ServiceBusToSourceRecordMapperTest {

  private static final String OUTPUT_TOPIC = "OUTPUT";
  private static final String MESSAGE_ID = "messageId";
  private static final long DELIVERY_COUNT = 10L;
  private static final OffsetDateTime TIME_NOW = OffsetDateTime.now();
  private static final String CONTENT_TYPE = "contentType";
  private static final String LABEL = AzureServiceBusSourceConnector.class.getSimpleName();
  private static final String CORRELATION_ID = "correlationId";
  private static final String PARTITION_KEY = "partitionKey";
  private static final String REPLY_TO = "replyTo";
  private static final String REPLY_TO_SESSION_ID = "replyToSessionId";
  private static final String DEAD_LETTER_SOURCE = "deadLetterSource";
  private static final Duration TIME_TO_LIVE = Duration.of(1, ChronoUnit.SECONDS);
  private static final Long SEQUENCE_NUMBER = 123L;
  private static final String SESSION_ID = "sessionId";
  private static final String LOCK_TOKEN = "lockToken";
  private static final byte[] MESSAGE_BODY = "messageBody".getBytes();
  private static final String GET_TO = "getTo";

  @Test
  void mapSingleSourceRecordWitAllParameters() {
    //given
    ServiceBusReceivedMessage busMessage = prepareMessageBusWithAllConsumedFields();
    AzureServiceBusPartitionKey partitionKey = new AzureServiceBusPartitionKey(OUTPUT_TOPIC, PARTITION_KEY);
    AzureServiceBusOffsetMarker busOffsetMarker = new AzureServiceBusOffsetMarker(SEQUENCE_NUMBER);

    //when
    SourceRecord sourceRecord =
        ServiceBusToSourceRecordMapper.mapSingleServiceBusMessage(busMessage, OUTPUT_TOPIC, partitionKey,
            busOffsetMarker);

    //then
    assertThat(sourceRecord)
        .returns(TIME_NOW.toEpochSecond(), from(SourceRecord::timestamp))
        .returns(partitionKey, from(SourceRecord::sourcePartition))
        .returns(null, from(SourceRecord::kafkaPartition))
        .returns(busOffsetMarker, from(SourceRecord::sourceOffset))
        .returns(OUTPUT_TOPIC, from(SourceRecord::topic))
        .returns(Schema.STRING_SCHEMA, from(SourceRecord::keySchema))
        .returns(ServiceBusToSourceRecordMapper.VALUE_SCHEMA, from(SourceRecord::valueSchema));

    Struct valueStruct = (Struct) sourceRecord.value();
    assertMappedStructValues(valueStruct);

  }

  private void assertMappedStructValues(Struct valueStruct) {
    assertThat(valueStruct)
        .returns(DELIVERY_COUNT, from(v -> v.get(ServiceBusValueSchemaField.DELIVERY_COUNT)))
        .returns(TIME_NOW.toEpochSecond(), from(v -> v.get(ServiceBusValueSchemaField.ENQUEUED_TIME_UTC)))
        .returns(CONTENT_TYPE, from(v -> v.get(ServiceBusValueSchemaField.CONTENT_TYPE)))
        .returns(LABEL, from(v -> v.get(ServiceBusValueSchemaField.LABEL)))
        .returns(CORRELATION_ID, from(v -> v.get(ServiceBusValueSchemaField.CORRELATION_ID)))
        .returns(PARTITION_KEY, from(v -> v.get(ServiceBusValueSchemaField.PARTITION_KEY)))
        .returns(REPLY_TO, from(v -> v.get(ServiceBusValueSchemaField.REPLY_TO)))
        .returns(REPLY_TO_SESSION_ID, from(v -> v.get(ServiceBusValueSchemaField.REPLY_TO_SESSION_ID)))
        .returns(DEAD_LETTER_SOURCE, from(v -> v.get(ServiceBusValueSchemaField.DEAD_LETTER_SOURCE)))
        .returns(TIME_TO_LIVE.toMillis(), from(v -> v.get(ServiceBusValueSchemaField.TIME_TO_LIVE)))
        .returns(TIME_NOW.toEpochSecond(), from(v -> v.get(ServiceBusValueSchemaField.LOCKED_UNTIL_UTC)))
        .returns(SEQUENCE_NUMBER, from(v -> v.get(ServiceBusValueSchemaField.SEQUENCE_NUMBER)))
        .returns(SESSION_ID, from(v -> v.get(ServiceBusValueSchemaField.SESSION_ID)))
        .returns(LOCK_TOKEN, from(v -> v.get(ServiceBusValueSchemaField.LOCK_TOKEN)))
        .returns(MESSAGE_BODY, from(v -> v.get(ServiceBusValueSchemaField.MESSAGE_BODY)))
        .returns(DELIVERY_COUNT, from(v -> v.get(ServiceBusValueSchemaField.DELIVERY_COUNT)));
  }

  private ServiceBusReceivedMessage prepareMessageBusWithAllConsumedFields() {
    ServiceBusReceivedMessage busReceivedMessage = mock(ServiceBusReceivedMessage.class);

    BinaryData bodyBinary = mock(BinaryData.class);
    when(bodyBinary.toBytes()).thenReturn(MESSAGE_BODY);

    Mockito.when(busReceivedMessage.getMessageId()).thenReturn(MESSAGE_ID);
    Mockito.when(busReceivedMessage.getDeliveryCount()).thenReturn(DELIVERY_COUNT);
    Mockito.when(busReceivedMessage.getEnqueuedTime()).thenReturn(TIME_NOW);
    Mockito.when(busReceivedMessage.getContentType()).thenReturn(CONTENT_TYPE);
    Mockito.when(busReceivedMessage.getCorrelationId()).thenReturn(CORRELATION_ID);
    Mockito.when(busReceivedMessage.getPartitionKey()).thenReturn(PARTITION_KEY);
    Mockito.when(busReceivedMessage.getReplyTo()).thenReturn(REPLY_TO);
    Mockito.when(busReceivedMessage.getReplyToSessionId()).thenReturn(REPLY_TO_SESSION_ID);
    Mockito.when(busReceivedMessage.getDeadLetterSource()).thenReturn(DEAD_LETTER_SOURCE);
    Mockito.when(busReceivedMessage.getTimeToLive()).thenReturn(TIME_TO_LIVE);
    Mockito.when(busReceivedMessage.getLockedUntil()).thenReturn(TIME_NOW);
    Mockito.when(busReceivedMessage.getSequenceNumber()).thenReturn(SEQUENCE_NUMBER);
    Mockito.when(busReceivedMessage.getSessionId()).thenReturn(SESSION_ID);
    Mockito.when(busReceivedMessage.getLockToken()).thenReturn(LOCK_TOKEN);
    Mockito.when(busReceivedMessage.getBody()).thenReturn(bodyBinary);
    Mockito.when(busReceivedMessage.getTo()).thenReturn(GET_TO);

    return busReceivedMessage;
  }
}

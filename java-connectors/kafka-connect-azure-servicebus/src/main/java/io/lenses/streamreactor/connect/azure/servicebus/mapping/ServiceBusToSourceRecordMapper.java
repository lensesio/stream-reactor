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

import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.CONTENT_TYPE;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.CORRELATION_ID;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.DEAD_LETTER_SOURCE;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.DELIVERY_COUNT;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.ENQUEUED_TIME_UTC;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.GET_TO;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.LABEL;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.LOCKED_UNTIL_UTC;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.LOCK_TOKEN;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.MESSAGE_BODY;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.PARTITION_KEY;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.REPLY_TO;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.REPLY_TO_SESSION_ID;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.SEQUENCE_NUMBER;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.SESSION_ID;
import static io.lenses.streamreactor.connect.azure.servicebus.mapping.ServiceBusValueSchemaField.TIME_TO_LIVE;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import io.lenses.streamreactor.connect.azure.servicebus.source.AzureServiceBusSourceConnector;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Class that maps {@link ServiceBusReceivedMessage} to Kafka Connect {@link SourceRecord}.
 */
public class ServiceBusToSourceRecordMapper {

  public static final Schema VALUE_SCHEMA;

  static {
    SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
    ServiceBusValueSchemaField.getAllFields()
        .forEach(field -> structSchemaBuilder.field(field.name(), field.schema()));
    VALUE_SCHEMA = structSchemaBuilder.build();
  }

  private ServiceBusToSourceRecordMapper() {
  }

  /**
   * Method to make SourceRecord out of ServiceBusReceivedMessage with key and body as structure.
   *
   * @param serviceBusMessage original Service Bus message
   * @param outputTopic       Output topic for record
   * @param partitionKey      AzureTopicPartitionKey to indicate topic and partition
   * @param offsetMap         AzureOffsetMarker to indicate offset
   * @return mapped SourceRecord
   */
  public static SourceRecord mapSingleServiceBusMessage(ServiceBusReceivedMessage serviceBusMessage, String outputTopic,
      Map<String, String> partitionKey, Map<String, Object> offsetMap) {
    String key = serviceBusMessage.getMessageId();
    Struct valueObject = new Struct(VALUE_SCHEMA);

    mapValuesFromServiceBusMessage(valueObject, serviceBusMessage);
    return new AzureServiceBusSourceRecord(partitionKey, offsetMap, outputTopic, null, Schema.STRING_SCHEMA, key,
        VALUE_SCHEMA, valueObject, serviceBusMessage.getEnqueuedTime().toEpochSecond(), new ConnectHeaders());
  }

  private static void mapValuesFromServiceBusMessage(Struct targetObject, ServiceBusReceivedMessage serviceBusMessage) {
    targetObject.put(DELIVERY_COUNT, serviceBusMessage.getDeliveryCount());
    targetObject.put(ENQUEUED_TIME_UTC, serviceBusMessage.getEnqueuedTime().toEpochSecond());
    targetObject.put(CONTENT_TYPE, serviceBusMessage.getContentType());
    targetObject.put(LABEL, AzureServiceBusSourceConnector.class.getSimpleName());
    targetObject.put(CORRELATION_ID, serviceBusMessage.getCorrelationId());
    targetObject.put(PARTITION_KEY, serviceBusMessage.getPartitionKey());
    targetObject.put(REPLY_TO, serviceBusMessage.getReplyTo());
    targetObject.put(REPLY_TO_SESSION_ID, serviceBusMessage.getReplyToSessionId());
    targetObject.put(DEAD_LETTER_SOURCE, serviceBusMessage.getDeadLetterSource());
    targetObject.put(TIME_TO_LIVE, serviceBusMessage.getTimeToLive().toMillis());
    targetObject.put(LOCKED_UNTIL_UTC, serviceBusMessage.getLockedUntil().toEpochSecond());
    targetObject.put(SEQUENCE_NUMBER, serviceBusMessage.getSequenceNumber());
    targetObject.put(SESSION_ID, serviceBusMessage.getSessionId());
    targetObject.put(LOCK_TOKEN, serviceBusMessage.getLockToken());
    targetObject.put(MESSAGE_BODY, serviceBusMessage.getBody().toBytes());
    targetObject.put(GET_TO, serviceBusMessage.getTo());
  }

}

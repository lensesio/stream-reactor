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
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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

    Struct valueObject = createStructFromServiceBusMessage(serviceBusMessage);
    return new AzureServiceBusSourceRecord(partitionKey, offsetMap, outputTopic, key,
        VALUE_SCHEMA, valueObject, Instant.now().toEpochMilli());
  }

  private static Struct createStructFromServiceBusMessage(final ServiceBusReceivedMessage serviceBusMessage) {
    Struct struct =
        new Struct(VALUE_SCHEMA)
            .put(DELIVERY_COUNT, serviceBusMessage.getDeliveryCount())
            .put(ENQUEUED_TIME_UTC, serviceBusMessage.getEnqueuedTime().toInstant().toEpochMilli())
            .put(LABEL, AzureServiceBusSourceConnector.class.getSimpleName())
            .put(TIME_TO_LIVE, serviceBusMessage.getTimeToLive().toMillis())
            .put(MESSAGE_BODY, serviceBusMessage.getBody().toBytes())
            .put(GET_TO, serviceBusMessage.getTo());

    addOptionalSchemaValues(struct, serviceBusMessage);

    return struct;
  }

  private static void addOptionalSchemaValues(Struct struct, ServiceBusReceivedMessage serviceBusMessage) {
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getContentType(), CONTENT_TYPE);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getCorrelationId(), CORRELATION_ID);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getPartitionKey(), PARTITION_KEY);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getReplyTo(), REPLY_TO);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getReplyToSessionId(), REPLY_TO_SESSION_ID);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getDeadLetterSource(), DEAD_LETTER_SOURCE);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getSequenceNumber(), SEQUENCE_NUMBER);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getSessionId(), SESSION_ID);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getLockToken(), LOCK_TOKEN);
    addOptionalSchemaValueIfExists(struct, serviceBusMessage.getTo(), GET_TO);

    Optional.ofNullable(serviceBusMessage.getLockedUntil())
        .ifPresent(lu -> struct.put(LOCKED_UNTIL_UTC, lu.toInstant().toEpochMilli()));

  }

  private static void addOptionalSchemaValueIfExists(Struct struct, Object value, Field field) {
    Optional.ofNullable(value).ifPresent(val -> struct.put(field, val));
  }

}

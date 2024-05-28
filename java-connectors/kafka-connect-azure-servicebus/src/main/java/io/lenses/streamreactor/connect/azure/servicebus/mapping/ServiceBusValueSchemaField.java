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

import java.util.List;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

/**
 * Class that represents Kafka Connects {@link Schema} for values from Service Bus.
 */
public class ServiceBusValueSchemaField {

  static final Field DELIVERY_COUNT =
      new Field(SchemaFieldConstants.DELIVERY_COUNT, 0, Schema.INT64_SCHEMA);
  static final Field ENQUEUED_TIME_UTC =
      new Field(SchemaFieldConstants.ENQUEUED_TIME_UTC, 1, Schema.INT64_SCHEMA);
  static final Field CONTENT_TYPE =
      new Field(SchemaFieldConstants.CONTENT_TYPE, 2, Schema.STRING_SCHEMA);
  static final Field LABEL =
      new Field(SchemaFieldConstants.LABEL, 3, Schema.STRING_SCHEMA);
  static final Field CORRELATION_ID =
      new Field(SchemaFieldConstants.CORRELATION_ID, 4, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field MESSAGE_PROPERTIES =
      new Field(SchemaFieldConstants.MESSAGE_PROPERTIES, 5, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field PARTITION_KEY =
      new Field(SchemaFieldConstants.PARTITION_KEY, 6, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field REPLY_TO =
      new Field(SchemaFieldConstants.REPLY_TO, 7, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field REPLY_TO_SESSION_ID =
      new Field(SchemaFieldConstants.REPLY_TO_SESSION_ID, 8, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field DEAD_LETTER_SOURCE =
      new Field(SchemaFieldConstants.DEAD_LETTER_SOURCE, 9, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field TIME_TO_LIVE =
      new Field(SchemaFieldConstants.TIME_TO_LIVE, 10, Schema.INT64_SCHEMA);
  static final Field LOCKED_UNTIL_UTC =
      new Field(SchemaFieldConstants.LOCKED_UNTIL_UTC, 11, Schema.OPTIONAL_INT64_SCHEMA);
  static final Field SEQUENCE_NUMBER =
      new Field(SchemaFieldConstants.SEQUENCE_NUMBER, 12, Schema.OPTIONAL_INT64_SCHEMA);
  static final Field SESSION_ID =
      new Field(SchemaFieldConstants.SESSION_ID, 13, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field LOCK_TOKEN =
      new Field(SchemaFieldConstants.LOCK_TOKEN, 14, Schema.OPTIONAL_STRING_SCHEMA);
  static final Field MESSAGE_BODY =
      new Field(SchemaFieldConstants.MESSAGE_BODY, 15, Schema.BYTES_SCHEMA);
  static final Field GET_TO =
      new Field(SchemaFieldConstants.GET_TO, 16, Schema.OPTIONAL_STRING_SCHEMA);

  private static final List<Field> ALL_FIELDS =
      List.of(
          DELIVERY_COUNT,
          ENQUEUED_TIME_UTC,
          CONTENT_TYPE,
          LABEL,
          CORRELATION_ID,
          MESSAGE_PROPERTIES,
          PARTITION_KEY,
          REPLY_TO,
          REPLY_TO_SESSION_ID,
          DEAD_LETTER_SOURCE,
          TIME_TO_LIVE,
          LOCKED_UNTIL_UTC,
          SEQUENCE_NUMBER,
          SESSION_ID,
          LOCK_TOKEN,
          MESSAGE_BODY,
          GET_TO);

  private ServiceBusValueSchemaField() {
  }

  public static List<Field> getAllFields() {
    return ALL_FIELDS;
  }

  private static class SchemaFieldConstants {

    private static final String DELIVERY_COUNT = "deliveryCount";
    private static final String ENQUEUED_TIME_UTC = "enqueuedTimeUtc";
    private static final String CONTENT_TYPE = "contentType";
    private static final String LABEL = "label";
    private static final String CORRELATION_ID = "correlationId";
    private static final String MESSAGE_PROPERTIES = "messageProperties";
    private static final String PARTITION_KEY = "partitionKey";
    private static final String REPLY_TO = "replyTo";
    private static final String REPLY_TO_SESSION_ID = "replyToSessionId";
    private static final String DEAD_LETTER_SOURCE = "deadLetterSource";
    private static final String TIME_TO_LIVE = "timeToLive";
    private static final String LOCKED_UNTIL_UTC = "lockedUntilUtc";
    private static final String SEQUENCE_NUMBER = "sequenceNumber";
    private static final String SESSION_ID = "sessionId";
    private static final String LOCK_TOKEN = "lockToken";
    private static final String MESSAGE_BODY = "messageBody";
    private static final String GET_TO = "getTo";
  }
}

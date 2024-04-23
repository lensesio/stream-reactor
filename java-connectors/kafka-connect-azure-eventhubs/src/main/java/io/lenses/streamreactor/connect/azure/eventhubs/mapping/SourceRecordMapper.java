/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.eventhubs.mapping;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * Class with utility method to convert to SourceRecord.
 */
public class SourceRecordMapper {

  /**
   * Method to make SourceRecord out of ConsumerRecord including optional byte headers from original
   * message.
   *
   * @param consumerRecord original consumer record
   * @param partitionKey   AzureTopicPartitionKey to indicate topic and partition
   * @param offsetMap      AzureOffsetMarker to indicate offset
   * @param outputTopic    Output topic for record
   * @param keySchema      Schema of the key
   * @param valueSchema    Schema of the value
   * @return SourceRecord with headers
   */
  public static SourceRecord mapSourceRecordIncludingHeaders(
      ConsumerRecord<?, ?> consumerRecord,
      Map<String, String> partitionKey, Map<String, Object> offsetMap,
      String outputTopic, Schema keySchema, Schema valueSchema) {
    Iterable<Header> headers = consumerRecord.headers();
    ConnectHeaders connectHeaders = new ConnectHeaders();
    for (Header header : headers) {
      connectHeaders.add(header.key(),
          new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA, header.value()));
    }
    return new SourceRecord(partitionKey, offsetMap,
        outputTopic, null, keySchema, consumerRecord.key(),
        valueSchema, consumerRecord.value(), consumerRecord.timestamp(),
        connectHeaders);
  }

  /**
   * Method to make SourceRecord out of ConsumerRecord including optional byte headers
   * from original message.
   *
   * @param consumerRecord original consumer record
   * @param partitionKey partitionKey to indicate topic and partition
   * @param offsetMap AzureOffsetMarker to indicate offset
   * @param outputTopic Output topic for record
   * @param keySchema Schema of the key
   * @param valueSchema Schema of the value
   * @return SourceRecord without headers
   */
  public static SourceRecord mapSourceRecordWithoutHeaders(
      ConsumerRecord<?, ?> consumerRecord,
      Map<String, String> partitionKey, Map<String, Object> offsetMap,
      String outputTopic, Schema keySchema, Schema valueSchema) {
    return new SourceRecord(partitionKey, offsetMap,
        outputTopic, null, keySchema, consumerRecord.key(),
        valueSchema, consumerRecord.value(), consumerRecord.timestamp(), null);
  }

}

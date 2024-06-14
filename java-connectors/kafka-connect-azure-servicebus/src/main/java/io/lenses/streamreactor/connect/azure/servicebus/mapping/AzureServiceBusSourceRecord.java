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

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import lombok.ToString;

/**
 * Implementation of {@link SourceRecord} for Microsoft Azure EventHubs.
 */
@ToString(callSuper = true)
public class AzureServiceBusSourceRecord extends SourceRecord {

  public AzureServiceBusSourceRecord(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset,
      String topic, Object key, Schema valueSchema, Object value, Long timestamp) {
    super(sourcePartition, sourceOffset, topic, null,
        Schema.STRING_SCHEMA, key, valueSchema, value, timestamp, new ConnectHeaders());
  }
}

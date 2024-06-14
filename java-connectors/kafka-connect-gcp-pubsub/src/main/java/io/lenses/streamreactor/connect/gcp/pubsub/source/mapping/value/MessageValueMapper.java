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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.value;

import org.apache.kafka.connect.data.Schema;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;

/**
 * MessageValueMapping is responsible for mapping the value from PubSubMessageData to Kafka Connect value.
 */
public class MessageValueMapper implements ValueMapper {

  @Override
  public Object mapValue(final PubSubMessageData source) {
    return source.getMessage().getData().toByteArray();
  }

  @Override
  public Schema getSchema() {
    return Schema.BYTES_SCHEMA;
  }
}

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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.key;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * CompatibilityKeyMapping is responsible for mapping the key from PubSubMessageData to Kafka Connect key.
 * The key is composed of the project id, topic id, subscription id and message id. The aim is to maintain compatibility
 * with another connector in the market.
 */
public class CompatibilityKeyMapping implements KeyMapping {

  private static final String STRUCT_KEY_PROJECT_ID = "ProjectId";
  private static final String STRUCT_KEY_TOPIC_ID = "TopicId";
  private static final String STRUCT_KEY_SUBSCRIPTION_ID = "SubscriptionId";
  private static final String STRUCT_KEY_MESSAGE_ID = "MessageId";

  private static final Schema SCHEMA =
      SchemaBuilder
          .struct()
          .field(STRUCT_KEY_PROJECT_ID, Schema.STRING_SCHEMA)
          .field(STRUCT_KEY_TOPIC_ID, Schema.STRING_SCHEMA)
          .field(STRUCT_KEY_SUBSCRIPTION_ID, Schema.STRING_SCHEMA)
          .field(STRUCT_KEY_MESSAGE_ID, Schema.STRING_SCHEMA)
          .build();

  @Override
  public Object getKey(final PubSubMessageData source) {
    return new Struct(SCHEMA)
        .put(STRUCT_KEY_PROJECT_ID, source.getSourcePartition().getProjectId())
        .put(STRUCT_KEY_TOPIC_ID, source.getSourcePartition().getTopicId())
        .put(STRUCT_KEY_SUBSCRIPTION_ID, source.getSourcePartition().getSubscriptionId())
        .put(STRUCT_KEY_MESSAGE_ID, source.getSourceOffset().getMessageId());
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }
}

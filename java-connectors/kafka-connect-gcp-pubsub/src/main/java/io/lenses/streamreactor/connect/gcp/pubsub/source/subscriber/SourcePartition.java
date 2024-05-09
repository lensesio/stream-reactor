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
package io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * SourcePartition holds the location from which the message was sourced within GCP PubSub, for use reporting back
 * partitions to Kafka Connect for later use in the GCP record acknowledgement..
 */
@Getter
@AllArgsConstructor
public class SourcePartition {

  private static final String KEY_PROJECT_ID = "project.id";
  private static final String KEY_TOPIC_ID = "topic.id";
  private static final String KEY_SUBSCRIPTION_ID = "subscription.id";

  private String projectId;
  private String topicId;
  private String subscriptionId;

  public static SourcePartition fromMap(Map<String, String> sourceLocation) {
    return new SourcePartition(
        sourceLocation.get(KEY_PROJECT_ID),
        sourceLocation.get(KEY_TOPIC_ID),
        sourceLocation.get(KEY_SUBSCRIPTION_ID)
    );
  }

  public Map<String, String> toMap() {
    return Map.of(
        KEY_TOPIC_ID, topicId,
        KEY_PROJECT_ID, projectId,
        KEY_SUBSCRIPTION_ID, subscriptionId
    );
  }

}

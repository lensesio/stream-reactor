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
package io.lenses.streamreactor.connect.gcp.pubsub.source.mapping.headers;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;
import lombok.val;

/**
 * MinimalAndMessageAttributesHeaderMapping is responsible for mapping minimal headers and message attributes from
 * PubSubMessageData to Kafka Connect headers.
 * It extends the functionality of MinimalHeaderMapping by adding message attributes to the headers.
 */
public class MinimalAndMessageAttributesHeaderMapper implements HeaderMapper {

  private final MinimalHeaderMapper minimalHeaderMapping = new MinimalHeaderMapper();

  private static final String HEADER_PROJECT_ID = "GCPProjectId";
  private static final String HEADER_TOPIC_ID = "PubSubTopicId";
  private static final String HEADER_SUBSCRIPTION_ID = "PubSubSubscriptionId";

  @Override
  public Map<String, String> mapHeaders(final PubSubMessageData source) {
    val miniMap = minimalHeaderMapping.mapHeaders(source);
    val extraMap = mapExtra(source);
    val headMap = source.getMessage().getAttributesMap();
    return ImmutableMap.<String, String>builder()
        .putAll(miniMap)
        .putAll(extraMap)
        .putAll(headMap)
        .build();
  }

  private Map<String, String> mapExtra(PubSubMessageData source) {
    return Map.of(
        HEADER_PROJECT_ID, source.getSourcePartition().getProjectId(),
        HEADER_TOPIC_ID, source.getSourcePartition().getTopicId(),
        HEADER_SUBSCRIPTION_ID, source.getSourcePartition().getSubscriptionId()
    );
  }
}

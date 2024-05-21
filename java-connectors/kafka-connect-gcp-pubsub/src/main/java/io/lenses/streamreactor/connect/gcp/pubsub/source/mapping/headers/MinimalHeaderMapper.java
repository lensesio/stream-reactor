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

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;

/**
 * MinimalHeaderMapping is responsible for mapping minimal headers from PubSubMessageData to Kafka Connect headers.
 * The minimal headers include only the most essential information from the PubSubMessageData.
 */
public class MinimalHeaderMapper implements HeaderMapper {

  @Override
  public Map<String, String> mapHeaders(final PubSubMessageData source) {
    return Map.of(
        "PublishTimestamp", String.valueOf(source.getMessage().getPublishTime().getSeconds())
    );
  }

}

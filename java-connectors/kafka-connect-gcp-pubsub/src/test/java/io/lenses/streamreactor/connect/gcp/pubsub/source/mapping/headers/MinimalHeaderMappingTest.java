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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.subscriber.PubSubMessageData;

@ExtendWith(MockitoExtension.class)
class MinimalHeaderMappingTest {

  private static final String PUBLISH_TIME = "1955-11-12T10:04:00Z";
  private static final Instant PUBLISH_TIME_INSTANT = Instant.parse(PUBLISH_TIME);

  @Mock
  private PubSubMessageData pubSubMessageData;

  @Mock
  private PubsubMessage pubsubMessage;

  private MinimalHeaderMapping minimalHeaderMapping;

  @BeforeEach
  void setup() {
    minimalHeaderMapping = new MinimalHeaderMapping();
  }

  @Test
  void testGetHeaders() {
    when(pubsubMessage.getPublishTime()).thenReturn(Timestamp.newBuilder().setSeconds(PUBLISH_TIME_INSTANT
        .getEpochSecond()).build());
    when(pubSubMessageData.getMessage()).thenReturn(pubsubMessage);

    Map<String, String> result = minimalHeaderMapping.getHeaders(pubSubMessageData);

    assertEquals(ImmutableMap.builder().put("PublishTimestamp", String.valueOf(PUBLISH_TIME_INSTANT.getEpochSecond()))
            .put("PublishDate", PUBLISH_TIME).build(), result);
  }
}

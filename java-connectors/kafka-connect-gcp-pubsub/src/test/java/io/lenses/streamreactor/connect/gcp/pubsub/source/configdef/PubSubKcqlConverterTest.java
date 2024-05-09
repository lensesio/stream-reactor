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
package io.lenses.streamreactor.connect.gcp.pubsub.source.configdef;

import static io.lenses.streamreactor.connect.gcp.pubsub.source.configdef.PubSubKcqlConverter.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

@ExtendWith(MockitoExtension.class)
class PubSubKcqlConverterTest {

  private static final String SUBSCRIPTION_ID = "test-subscription";
  private static final String TOPIC_NAME = "test-topic";
  private static final String TARGET_KAFKA_TOPIC = "test-kafka-topic";
  private static final int BATCH_SIZE = 999;
  private static final long TTL = 1599L;

  @Mock
  private PubSubService pubSubService;

  @Mock
  private Kcql kcql;

  private PubSubKcqlConverter pubSubKcqlConverter;

  @BeforeEach
  public void setup() {
    pubSubKcqlConverter = new PubSubKcqlConverter(pubSubService);
  }

  @Test
  void convertShouldMapKcqlPropertiesToPubSubSubscription() {
    setUpScenario(
        Optional.of(String.valueOf(BATCH_SIZE)),
        Optional.of(String.valueOf(TTL))
    );

    PubSubSubscription result = pubSubKcqlConverter.convert(kcql);

    assertEquals(TOPIC_NAME, result.getSourceTopicId());
    assertEquals(TARGET_KAFKA_TOPIC, result.getTargetKafkaTopic());
    assertEquals(SUBSCRIPTION_ID, result.getSubscriptionId());
    assertEquals(BATCH_SIZE, result.getBatchSize());
    assertEquals(TTL, result.getCacheExpire());
  }

  @Test
  void convertShouldProvideDefaultsForBatchSizeAndCacheTtl() {
    setUpScenario(
        Optional.empty(),
        Optional.empty()
    );

    PubSubSubscription result = pubSubKcqlConverter.convert(kcql);

    assertEquals(DEFAULT_BATCH_SIZE, result.getBatchSize());
    assertEquals(DEFAULT_CACHE_TTL_MILLIS, result.getCacheExpire());
  }

  private void setUpScenario(Optional<String> maybeBatchSize, Optional<String> maybeCacheTtl) {
    when(kcql.getSource()).thenReturn(SUBSCRIPTION_ID);
    when(kcql.getTarget()).thenReturn(TARGET_KAFKA_TOPIC);
    when(pubSubService.topicNameFor(SUBSCRIPTION_ID)).thenReturn(TOPIC_NAME);
    when(kcql.extractOptionalProperty(KCQL_PROP_KEY_BATCH_SIZE)).thenReturn(maybeBatchSize);
    when(kcql.extractOptionalProperty(KCQL_PROP_KEY_CACHE_TTL)).thenReturn(maybeCacheTtl);
  }
}

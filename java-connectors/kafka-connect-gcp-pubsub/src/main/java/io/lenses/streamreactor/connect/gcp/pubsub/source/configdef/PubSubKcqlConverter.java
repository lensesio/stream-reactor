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

import org.apache.kafka.common.config.ConfigException;

import io.lenses.kcql.Kcql;
import io.lenses.streamreactor.common.config.base.intf.KcqlConverter;
import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import lombok.val;

/**
 * PubSubKcqlConverter is responsible for converting Kcql source to PubSubSubscription.
 * It uses the builder pattern to create a new instance of PubSubSubscription.
 */
public class PubSubKcqlConverter extends KcqlConverter<PubSubSubscription> {

  public static final int DEFAULT_BATCH_SIZE = 1000;
  public static final long DEFAULT_CACHE_TTL_MILLIS = 3600L * 1000L;
  public static final int DEFAULT_CACHE_MAX = 10000;

  // 1 hour
  public static final String KCQL_PROP_KEY_BATCH_SIZE = "batch.size";
  public static final String KCQL_PROP_KEY_CACHE_TTL = "cache.ttl";
  public static final String KCQL_PROP_KEY_QUEUE_MAX = "queue.max";

  private final PubSubService pubSubService;

  public PubSubKcqlConverter(PubSubService pubSubService) {
    this.pubSubService = pubSubService;
  }

  public PubSubSubscription convert(Kcql source) throws ConfigException {
    try {
      source.validateKcqlProperties(KCQL_PROP_KEY_BATCH_SIZE, KCQL_PROP_KEY_CACHE_TTL, KCQL_PROP_KEY_QUEUE_MAX);
    } catch (IllegalArgumentException e) {
      throw new ConfigException("Invalid KCQL properties", e);
    }
    val subscriptionId = source.getSource();
    return PubSubSubscription.builder()
        .sourceTopicId(pubSubService.topicNameFor(subscriptionId))
        .targetKafkaTopic(source.getTarget())
        .subscriptionId(subscriptionId)
        .batchSize(source.extractOptionalProperty(KCQL_PROP_KEY_BATCH_SIZE).map(Integer::parseInt).orElse(
            DEFAULT_BATCH_SIZE))
        .cacheExpire(source.extractOptionalProperty(KCQL_PROP_KEY_CACHE_TTL).map(Long::parseLong).orElse(
            DEFAULT_CACHE_TTL_MILLIS))
        .queueMaxEntries(source.extractOptionalProperty(KCQL_PROP_KEY_QUEUE_MAX).map(Integer::parseInt).orElse(
            DEFAULT_CACHE_MAX))
        .build();
  }
}

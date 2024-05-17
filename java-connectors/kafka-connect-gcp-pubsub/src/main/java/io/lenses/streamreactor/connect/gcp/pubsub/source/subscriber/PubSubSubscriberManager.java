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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * SubscriberManager is responsible for managing the subscribers and polling messages from GCP PubSub.
 */
@Slf4j
public class PubSubSubscriberManager {

  /**
   * Map of Subscribers. The key is the subscriptionId.
   */
  private final Map<String, PubSubSubscriber> subscribers;

  public PubSubSubscriberManager(
      PubSubService pubSubService,
      String projectId,
      List<PubSubSubscription> subscriptionConfigs,
      SubscriberCreator subscriberCreator
  ) {
    log.info("Starting PubSubSubscriberManager for {} subscriptions", subscriptionConfigs.size());
    subscribers =
        subscriptionConfigs
            .parallelStream()
            .collect(Collectors.toConcurrentMap(
                PubSubSubscription::getSubscriptionId,
                s -> subscriberCreator.createSubscriber(pubSubService, projectId, s)));
  }

  public List<PubSubMessageData> poll() {
    log.trace("Polling messages from all partitions");
    val subs =
        subscribers
            .values()
            .parallelStream()
            .flatMap(pubSubSubscriber -> pubSubSubscriber.getMessages().stream())
            .collect(Collectors.toList());
    log.debug("Polled {} messages from all partitions", subs.size());
    return subs;
  }

  public void commitRecord(
      SourcePartition sourcePartition,
      SourceOffset sourceOffset
  ) {
    log.trace("Committing record for partition {} with offset {}", sourcePartition, sourceOffset);
    subscribers
        .get(sourcePartition.getSubscriptionId())
        .acknowledge(sourceOffset.getMessageId());
  }

  public void stop() {
    log.info("Stopping PubSubSubscriberManager");
    subscribers.values().forEach(PubSubSubscriber::stopAsync);
  }

}

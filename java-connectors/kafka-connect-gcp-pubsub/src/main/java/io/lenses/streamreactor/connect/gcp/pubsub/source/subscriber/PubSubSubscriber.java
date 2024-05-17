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
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Subscriber is responsible for receiving messages from GCP PubSub and storing them in a queue for processing.
 */
@Slf4j
public class PubSubSubscriber {

  private final Queue<PubsubMessage> messageQueue;

  private final SourcePartition sourcePartition;

  private final Cache<String, AckReplyConsumer> ackCache;

  private final Integer batchSize;

  private final String targetTopicName;

  private final com.google.cloud.pubsub.v1.Subscriber gcpSubscriber;

  public PubSubSubscriber(
      PubSubService pubSubService,
      String projectId,
      PubSubSubscription subscription
  ) {
    log.info("Starting PubSubSubscriber for subscription {}", subscription.getSubscriptionId());
    targetTopicName = subscription.getTargetKafkaTopic();
    batchSize = subscription.getBatchSize();
    messageQueue = new ConcurrentLinkedQueue<>();
    ackCache =
        Caffeine
            .newBuilder()
            .expireAfterWrite(subscription.getCacheExpire(), TimeUnit.MILLISECONDS)
            .build();

    val receiver = createMessageReceiver();

    gcpSubscriber = pubSubService.createSubscriber(subscription.getSubscriptionId(), receiver);
    sourcePartition =
        new SourcePartition(
            projectId,
            subscription.getSourceTopicId(),
            subscription.getSubscriptionId()
        );
    startAsync();
  }

  public void startAsync() {
    gcpSubscriber.startAsync();
  }

  private MessageReceiver createMessageReceiver() {
    return (PubsubMessage message, AckReplyConsumer consumer) -> {
      messageQueue.add(message);
      ackCache.put(message.getMessageId(), consumer);
    };
  }

  public List<PubSubMessageData> getMessages() {
    return IntStream.range(0, batchSize)
        .mapToObj(i -> messageQueue.poll())
        .filter(Objects::nonNull)
        .map(psm -> new PubSubMessageData(
            sourcePartition,
            new SourceOffset(psm.getMessageId()),
            psm,
            targetTopicName
        ))
        .collect(Collectors.toUnmodifiableList());
  }

  public void acknowledge(String messageId) {
    log.trace("Sending acknowledgement for {}}", messageId);
    Optional
        .ofNullable(ackCache.getIfPresent(messageId))
        .ifPresent(e -> {
          e.ack();
          ackCache.invalidate(messageId);
        });
  }

  public void stopAsync() {
    gcpSubscriber.stopAsync();
  }

}

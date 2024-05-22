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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@ExtendWith(MockitoExtension.class)
@Slf4j
class PubSubSubscriberTest {

  private static final String MESSAGE_ID_PREFIX = "message_id_";
  private static final String TARGET_TOPIC = "target-topic";
  private static final int QUEUE_SIZE = 100;
  private static final int BATCH_SIZE = 10;
  private static final String SUBSCRIPTION_ID = "subscription-id";
  private static final String SOURCE_TOPIC_ID = "source-topic-id";
  private static final long CACHE_EXPIRE = 1000L;

  @Mock
  private PubSubSubscription subscription;
  @Mock
  private PubSubService pubSubService;
  @Mock
  private Subscriber gcpSubscriber;

  @BeforeEach
  void setUp() {

    when(pubSubService.createSubscriber(anyString(), any(MessageReceiver.class)))
        .thenReturn(gcpSubscriber);

    when(subscription.getSubscriptionId()).thenReturn(SUBSCRIPTION_ID);
    when(subscription.getTargetKafkaTopic()).thenReturn(TARGET_TOPIC);
    when(subscription.getBatchSize()).thenReturn(BATCH_SIZE);
    when(subscription.getQueueMaxEntries()).thenReturn(QUEUE_SIZE);
    when(subscription.getCacheExpire()).thenReturn(CACHE_EXPIRE);
    when(subscription.getSourceTopicId()).thenReturn(SOURCE_TOPIC_ID);

  }

  @Test
  void testStartAsync() {
    createPubSubSubscriber();

    verify(gcpSubscriber, times(1)).startAsync();
  }

  @Test
  void testMessageReceptionAndQueueing() {

    val subscriber = createPubSubSubscriber();
    messageSend(1);

    List<PubSubMessageData> messages = subscriber.getMessages();

    assertEquals(1, messages.size());
    assertEquals(MESSAGE_ID_PREFIX + "0", messages.get(0).getSourceOffset().getMessageId());
    assertEquals(TARGET_TOPIC, messages.get(0).getTargetTopicName());
  }

  @Test
  void testNotAcknoledgeMessageOnFullQueue() {

    val subscriber = createPubSubSubscriber();
    // put 200 messages into the queue so it is twice oversaturated
    val acks = messageSend(QUEUE_SIZE * 2);

    // first 100 messages are added to the queue
    acks.subList(0, 99).forEach(ack -> {
      log.info("ACK: {}", ack);
      verifyNoInteractions(ack);
    });
    // all other messages bounce with a 'nack'
    acks.subList(100, 199).forEach(nack -> {
      log.info("NACK: {}", nack);
      verify(nack).nack();
    });

    val messages =
        IntStream.rangeClosed(0, BATCH_SIZE - 1)
            .mapToObj(i -> subscriber.getMessages())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    assertEquals(QUEUE_SIZE, messages.size());

  }

  @Test
  void testAcknowledgeMessage() {

    val acksIndexes = Set.of(0, 3, 5);
    val noAcksIndexes =
        IntStream
            .rangeClosed(0, QUEUE_SIZE - 1)
            .filter(i -> !acksIndexes.contains(i));

    val subscriber = createPubSubSubscriber();
    val acks = messageSend(QUEUE_SIZE);

    acksIndexes
        .forEach(i -> subscriber.acknowledge(MESSAGE_ID_PREFIX + i)
        );

    acksIndexes
        .forEach(i -> verify(acks.get(i)).ack()
        );

    noAcksIndexes.forEach(
        i -> verifyNoInteractions(acks.get(i))
    );
  }

  @Test
  void testStopAsync() {
    PubSubSubscriber subscriber = createPubSubSubscriber();
    reset(gcpSubscriber);
    subscriber.stopAsync();
    verify(gcpSubscriber, times(1)).stopAsync();
  }

  private PubSubSubscriber createPubSubSubscriber() {
    return new PubSubSubscriber(pubSubService, "project-id", subscription);
  }

  private List<AckReplyConsumer> messageSend(int numMessages) {

    final var receiver = captureMessageReceiver();

    return IntStream.rangeClosed(0, numMessages - 1).mapToObj(i -> {
      val ackReplyConsumer = mock(AckReplyConsumer.class);
      val message = PubsubMessage.newBuilder().setMessageId(MESSAGE_ID_PREFIX + i).build();

      receiver.receiveMessage(message, ackReplyConsumer);

      return ackReplyConsumer;
    }).collect(Collectors.toList());

  }

  private MessageReceiver captureMessageReceiver() {
    val receiverCaptor = ArgumentCaptor.forClass(MessageReceiver.class);
    verify(pubSubService).createSubscriber(eq(SUBSCRIPTION_ID), receiverCaptor.capture());
    return receiverCaptor.getValue();
  }

}

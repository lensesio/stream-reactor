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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import io.lenses.streamreactor.connect.gcp.pubsub.source.admin.PubSubService;
import io.lenses.streamreactor.connect.gcp.pubsub.source.config.PubSubSubscription;
import lombok.val;

@ExtendWith(MockitoExtension.class)
class PubSubSubscriberManagerTest {

  public static final String MESSAGE_ID_PREFIX = "myMessageId";
  public static final String PROJECT_ID = "myProject";
  public static final String TOPIC_ID = "myTopic";
  public static final String SUBSCRIPTION_ID_PREFIX = "mySubscription";
  public static final String TARGET_KAFKA_TOPIC_PREFIX = "targetKafkaTopic";

  @Mock
  private PubSubService pubSubService;

  @Mock
  private PubSubSubscriber pubSubSubscriber1;
  @Mock
  private PubSubSubscriber pubSubSubscriber2;

  @Mock
  private SubscriberCreator subscriberCreator;

  private PubSubSubscriberManager target;

  @BeforeEach
  void setUp() {

    final var subscription1 = mockSubscriberCreator(pubSubSubscriber1, "1");
    final var subscription2 = mockSubscriberCreator(pubSubSubscriber2, "2");

    val subscriptions = List.of(subscription1, subscription2);

    target = new PubSubSubscriberManager(pubSubService, PROJECT_ID, subscriptions, subscriberCreator);
  }

  @Test
  void testSingleRecordPoll() {

    val testMessage = generateTestMessage("1");

    when(pubSubSubscriber1.getMessages()).thenReturn(List.of(testMessage));

    val messages = target.poll();

    assertEquals(1, messages.size());
    assertEquals("test message1", messages.get(0).getMessage().getData().toStringUtf8());
  }

  @Test
  void testMultiRecordPoll() {

    val testMessage1 = generateTestMessage("1");
    val testMessage2 = generateTestMessage("2");

    when(pubSubSubscriber1.getMessages()).thenReturn(List.of(testMessage1, testMessage2));

    val messages = target.poll();

    assertEquals(2, messages.size());
    assertEquals("test message1", messages.get(0).getMessage().getData().toStringUtf8());
    assertEquals("test message2", messages.get(1).getMessage().getData().toStringUtf8());
  }

  private PubSubMessageData generateTestMessage(String id) {
    return new PubSubMessageData(
        sourcePartition(id),
        sourceOffset(id),
        PubsubMessage.newBuilder().setData(ByteString.copyFrom(("test message" + id).getBytes())).build(),
        TARGET_KAFKA_TOPIC_PREFIX
    );
  }

  @Test
  void testCommitRecord() {

    target.commitRecord(sourcePartition("1"), sourceOffset("1"));

    verify(pubSubSubscriber1).acknowledge(MESSAGE_ID_PREFIX + "1");
    verifyNoInteractions(pubSubSubscriber2);

  }

  @Test
  void testStop() {
    target.stop();

    verify(pubSubSubscriber1).stopAsync();
    verify(pubSubSubscriber2).stopAsync();
  }

  private PubSubSubscription mockSubscriberCreator(PubSubSubscriber pubSubSubscriber, String id) {
    val subscription =
        PubSubSubscription.builder()
            .targetKafkaTopic(PubSubSubscriberManagerTest.TARGET_KAFKA_TOPIC_PREFIX + id)
            .subscriptionId(PubSubSubscriberManagerTest.SUBSCRIPTION_ID_PREFIX + id)
            .cacheExpire(1000L)
            .build();

    when(subscriberCreator.createSubscriber(pubSubService, PROJECT_ID, subscription))
        .thenReturn(pubSubSubscriber);

    return subscription;
  }

  private PubSubSourcePartition sourcePartition(String id) {
    return new PubSubSourcePartition(PROJECT_ID, TOPIC_ID, SUBSCRIPTION_ID_PREFIX + id);
  }

  private PubSubSourceOffset sourceOffset(String id) {
    return new PubSubSourceOffset(MESSAGE_ID_PREFIX + id);
  }

}

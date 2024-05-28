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
package io.lenses.streamreactor.connect.gcp.pubsub.source.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.google.pubsub.v1.SubscriptionName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;

import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode;
import lombok.val;

@ExtendWith(MockitoExtension.class)
class PubSubServiceTest {

  private static final String PROJECT_ID = "test-project";
  private static final String SUBSCRIPTION_ID = "test-subscription";
  private static final String TOPIC_ID = "test-topic";

  private static final SubscriptionName SUBSCRIPTION_NAME =
      SubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);

  private static final ProjectSubscriptionName PROJECT_SUBSCRIPTION_NAME =
      ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);
  @Mock
  private AuthMode authMode;

  @Mock
  private SubscriptionAdminClient subscriptionAdminClient;

  @Mock
  private MessageReceiver messageReceiver;

  private PubSubService pubSubService;

  @BeforeEach
  public void setup() throws IOException {
    pubSubService = new PubSubService(authMode, PROJECT_ID, subscriptionAdminClient);
  }

  @Test
  void testCreateSubscriber() {
    val subscriber = pubSubService.createSubscriber(SUBSCRIPTION_ID, messageReceiver);
    assertEquals(PROJECT_SUBSCRIPTION_NAME.toString(), subscriber.getSubscriptionNameString());
  }

  @Test
  void testTopicNameFor() {
    val subscription = Subscription.newBuilder().setTopic(TOPIC_ID).build();
    when(subscriptionAdminClient.getSubscription(SUBSCRIPTION_NAME)).thenReturn(subscription);

    val topicName = pubSubService.topicNameFor(SUBSCRIPTION_ID);

    assertEquals(TOPIC_ID, topicName);
    verify(subscriptionAdminClient).getSubscription(SUBSCRIPTION_NAME);
  }
}

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

import java.io.IOException;
import java.util.Optional;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.SubscriptionName;

import io.lenses.streamreactor.connect.gcp.common.auth.mode.AuthMode;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

/**
 * PubSubService manages the remote PubSub resources and provides methods to create a subscriber and look up topic name.
 */
@Slf4j
public class PubSubService {

  private final SubscriptionAdminClient subscriptionAdminClient;

  private final AuthMode authMode;

  private final String projectId;

  public PubSubService(final AuthMode authMode, final String projectId) throws IOException {
    this.authMode = authMode;
    this.projectId = projectId;
    this.subscriptionAdminClient = createSubscriptionAdminClient(authMode);
  }

  public PubSubService(final AuthMode authMode, final String projectId,
      final SubscriptionAdminClient subscriptionAdminClient) {
    this.authMode = authMode;
    this.projectId = projectId;
    this.subscriptionAdminClient = subscriptionAdminClient;
  }

  public Subscriber createSubscriber(
      final String subscriptionId,
      final MessageReceiver receiver
  ) {
    val subscriberBuilder = Subscriber.newBuilder(createProjectSubscriptionName(subscriptionId), receiver);
    Optional.ofNullable(authMode).ifPresent(e -> subscriberBuilder.setCredentialsProvider(e::getCredentials));
    return subscriberBuilder.build();
  }

  private SubscriptionAdminClient createSubscriptionAdminClient(final AuthMode authMode) throws IOException {
    val settingsBuilder = SubscriptionAdminSettings.newBuilder();
    Optional.ofNullable(authMode).ifPresent(e -> settingsBuilder.setCredentialsProvider(e::getCredentials));
    return SubscriptionAdminClient.create(settingsBuilder.build());
  }

  public String topicNameFor(final String subscriptionId) {
    val subscription = subscriptionAdminClient.getSubscription(createSubscriptionName(subscriptionId));
    log.info("Found topic details {} for subscriptionId {}", subscription.getTopic(), subscriptionId);
    return subscription.getTopic();
  }

  private SubscriptionName createSubscriptionName(final String subscriptionId) {
    return SubscriptionName.of(projectId, subscriptionId);
  }

  private ProjectSubscriptionName createProjectSubscriptionName(final String subscriptionId) {
    return ProjectSubscriptionName.of(projectId, subscriptionId);
  }

}

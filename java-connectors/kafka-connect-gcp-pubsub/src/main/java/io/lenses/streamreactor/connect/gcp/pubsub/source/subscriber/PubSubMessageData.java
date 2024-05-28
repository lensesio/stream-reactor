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

import com.google.pubsub.v1.PubsubMessage;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * PubSubMessageData holds the data from PubSubMessage and the target topic name for processing before being sent back
 * via Kafka Connect.
 */
@AllArgsConstructor
@Getter
public class PubSubMessageData {

  private PubSubSourcePartition sourcePartition;

  private PubSubSourceOffset sourceOffset;

  private PubsubMessage message;

  private String targetTopicName;

}

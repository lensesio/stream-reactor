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

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * SourceOffset holds the message id from the PubSub message, to allow kafka connect to track the offset of the message,
 * for later use in the GCP offset acknowledgement.
 */
@AllArgsConstructor
@Getter
@ToString
public class SourceOffset {

  private static final String KEY_MESSAGE_ID = "message.id";
  private String messageId;

  public static SourceOffset fromMap(Map<String, String> sourceLocation) {
    return new SourceOffset(sourceLocation.get(KEY_MESSAGE_ID));
  }

  public Map<String, String> toMap() {
    return Map.of(KEY_MESSAGE_ID, messageId);
  }

}
